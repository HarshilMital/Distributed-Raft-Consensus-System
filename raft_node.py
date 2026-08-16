"""A Raft node exposing a replicated key-value store over gRPC.

Implements leader election, log replication and crash recovery as described
in the Raft paper, plus the leader-lease optimisation used by systems such
as CockroachDB: a leader may only serve reads while it holds a lease that a
quorum of followers has recently renewed, and a newly elected leader waits
for the previous lease to expire before serving. That combination gives
linearisable reads without paying a round trip per read.

Run one process per node:

    python raft_node.py --id 1

Peer addresses come from :mod:`config` (``RAFT_PEERS``, ``cluster.json``,
or a localhost default).
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import threading
import time
from concurrent import futures
from typing import Optional

import grpc

import config
import raft_pb2
import raft_pb2_grpc
from storage import NodeStorage

# Roles a node can be in. ``TEMP_LEADER`` is the interval between winning an
# election and acquiring the lease: the node has a quorum of votes but must
# wait out the old leader's lease before it may act as leader.
FOLLOWER = "follower"
CANDIDATE = "candidate"
TEMP_LEADER = "temp_leader"
LEADER = "leader"

NO_OP = "NO-OP"


class RaftNode(raft_pb2_grpc.RaftServicer):
    """One member of the Raft cluster.

    All mutable state is guarded by ``self._lock``. RPCs are always issued
    with the lock released so that a slow or unreachable peer cannot block
    incoming requests.
    """

    def __init__(
        self,
        node_id: int,
        peers: list[str],
        data_dir: Optional[str] = None,
    ) -> None:
        self.id = node_id
        self.peers = peers
        self.peer_ids = list(range(1, len(peers) + 1))
        self.majority = config.majority(len(peers))

        self.storage = NodeStorage(node_id, data_dir)
        self.log = logging.getLogger(f"raft.node{node_id}")
        self._configure_logging()

        self._lock = threading.RLock()

        # Persistent state, restored from disk below.
        self.currentTerm = 0
        self.votedFor: Optional[int] = None
        self.commitLength = 0
        self.entries: list[dict] = []  # [{'term': int, 'command': str}, ...]

        # Volatile state.
        self.currentRole = FOLLOWER
        self.currentLeader: Optional[int] = None
        self.votesReceived: set[int] = set()
        self.sentLength: dict[int, int] = {}
        self.ackedLength: dict[int, int] = {}
        self.db: dict[str, str] = {}  # the replicated state machine
        self.lease_acks = 0  # followers that renewed the lease this round

        self.election_timeout = self._next_election_timeout()
        self.lease_timeout = time.time() + config.LEASE_TIMEOUT

        # One long-lived channel per peer; far cheaper than dialling per RPC.
        self._stubs = {
            peer_id: raft_pb2_grpc.RaftStub(
                grpc.insecure_channel(peers[peer_id - 1])
            )
            for peer_id in self.peer_ids
            if peer_id != self.id
        }

        self._load_persistent_state()

        self._loop_thread = threading.Thread(target=self._run, daemon=True)
        self._loop_thread.start()

    # ------------------------------------------------------------------
    # Setup and persistence
    # ------------------------------------------------------------------

    def _configure_logging(self) -> None:
        """Mirror every event to stdout and to the node's dump.txt."""
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        if self.log.handlers:
            return

        formatter = logging.Formatter("%(asctime)s [node %(name)s] %(message)s")
        for handler in (
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(self.storage.dump_path),
        ):
            handler.setFormatter(formatter)
            self.log.addHandler(handler)

    def _load_persistent_state(self) -> None:
        """Restore term, vote, log and state machine after a restart."""
        self.currentTerm, self.votedFor, self.commitLength = (
            self.storage.load_metadata()
        )
        self.entries = self.storage.load_log()
        self.storage.save_metadata(
            self.currentTerm, self.votedFor, self.commitLength
        )

        # Replay the committed prefix to rebuild the state machine.
        for entry in self.entries[: self.commitLength]:
            self._apply(entry)

        self.log.info(
            "Recovered term %d, %d log entries, %d committed.",
            self.currentTerm,
            len(self.entries),
            self.commitLength,
        )

    def _persist_metadata(self) -> None:
        self.storage.save_metadata(
            self.currentTerm, self.votedFor, self.commitLength
        )

    def _set_term(self, term: int, voted_for: Optional[int] = None) -> None:
        self.currentTerm = term
        self.votedFor = voted_for
        self._persist_metadata()

    def _apply(self, entry: dict) -> None:
        """Apply a committed entry to the key-value state machine."""
        parts = entry["command"].split()
        if parts and parts[0] == "SET":
            self.db[parts[1]] = parts[2]

    def _next_election_timeout(self) -> float:
        """A randomised deadline, so that nodes rarely campaign together."""
        jitter = random.randint(1, 10) * 0.001
        return time.time() + random.randint(
            config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX
        ) + jitter

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        self.log.info("Node %d started with %d peers.", self.id, len(self.peers))
        while True:
            role = self.currentRole
            if role == FOLLOWER:
                self._tick_follower()
            elif role == TEMP_LEADER:
                self._tick_temp_leader()
            elif role == LEADER:
                self._tick_leader()
            time.sleep(0.05)

    def _tick_follower(self) -> None:
        """Start an election once the election timer expires."""
        if time.time() <= self.election_timeout:
            return

        with self._lock:
            self.log.info(
                "Node %d election timer timed out, Starting election.", self.id
            )
            self.currentRole = CANDIDATE
            self._set_term(self.currentTerm + 1, voted_for=self.id)
            self.votesReceived = {self.id}

        self._collect_votes()

        with self._lock:
            self.election_timeout = self._next_election_timeout()
            if self.currentRole == CANDIDATE:
                # Lost or split vote: back off and wait for the next timeout.
                self.currentRole = FOLLOWER

    def _tick_temp_leader(self) -> None:
        """Take over once the previous leader's lease has expired."""
        if time.time() <= self.lease_timeout:
            return

        with self._lock:
            self.log.info(
                "Node %d became the leader for term %d.", self.id, self.currentTerm
            )
            self.currentRole = LEADER
            self.currentLeader = self.id
            self.election_timeout = self._next_election_timeout()

            # A no-op entry in the new term lets the leader commit entries
            # inherited from previous terms (Raft §5.4.2).
            self._append_local(NO_OP)
            self.ackedLength[self.id] = len(self.entries)
            for peer_id in self.peer_ids:
                if peer_id != self.id:
                    self.sentLength[peer_id] = len(self.entries)
                    self.ackedLength[peer_id] = 0
            followers = [p for p in self.peer_ids if p != self.id]

        for peer_id in followers:
            self._replicate_log(peer_id)

    def _tick_leader(self) -> None:
        """Heartbeat, or step down if the lease could not be renewed."""
        if time.time() > self.lease_timeout:
            with self._lock:
                self.log.info(
                    "Leader %d lease renewal failed. Stepping Down.", self.id
                )
                self._step_down()
                self.lease_timeout = time.time() + config.LEASE_TIMEOUT
            return

        time.sleep(config.HEARTBEAT_INTERVAL)
        with self._lock:
            if self.currentRole != LEADER:
                return
            self.log.info("Leader %d sending heartbeat & Renewing Lease", self.id)
            self.lease_acks = 1  # the leader counts towards its own quorum
            followers = [p for p in self.peer_ids if p != self.id]

        for peer_id in followers:
            self._replicate_log(peer_id)

    def _step_down(self) -> None:
        """Revert to follower. Caller must hold the lock."""
        self.currentRole = FOLLOWER
        self.currentLeader = None
        self.votesReceived = set()
        self.election_timeout = self._next_election_timeout()

    # ------------------------------------------------------------------
    # Leader election
    # ------------------------------------------------------------------

    def RequestVote(self, request, context):  # gRPC handler
        with self._lock:
            if request.term > self.currentTerm:
                self._set_term(request.term)
                self.currentRole = FOLLOWER

            last_term = self.entries[-1]["term"] if self.entries else 0
            log_ok = request.lastLogTerm > last_term or (
                request.lastLogTerm == last_term
                and request.lastLogIndex >= len(self.entries)
            )
            can_vote = self.votedFor is None or self.votedFor == request.candidateId

            granted = request.term == self.currentTerm and can_vote and log_ok
            if granted:
                self._set_term(self.currentTerm, voted_for=request.candidateId)
                self.election_timeout = self._next_election_timeout()

            self.log.info(
                "Vote %s for Node %d in term %d.",
                "granted" if granted else "denied",
                request.candidateId,
                request.term,
            )
            # The candidate must not act as leader until this lease expires.
            return raft_pb2.RequestVoteResponse(
                term=self.currentTerm,
                voteGranted=granted,
                leaseDuration=max(0.0, self.lease_timeout - time.time()),
            )

    def _collect_votes(self) -> None:
        """Campaign for the current term, one peer at a time."""
        with self._lock:
            request = raft_pb2.RequestVoteRequest(
                term=self.currentTerm,
                candidateId=self.id,
                lastLogIndex=len(self.entries),
                lastLogTerm=self.entries[-1]["term"] if self.entries else 0,
            )
            followers = [p for p in self.peer_ids if p != self.id]

        for peer_id in followers:
            try:
                response = self._stubs[peer_id].RequestVote(
                    request, timeout=config.RPC_TIMEOUT
                )
            except grpc.RpcError:
                self.log.info(
                    "Error occurred while sending RPC to Node %d.", peer_id
                )
                continue

            with self._lock:
                # Never act as leader while any peer still owes the old
                # leader a lease.
                self.lease_timeout = max(
                    time.time() + response.leaseDuration, self.lease_timeout
                )

                if response.term > self.currentTerm:
                    self._set_term(response.term)
                    self._step_down()
                    return

                if (
                    self.currentRole == CANDIDATE
                    and response.voteGranted
                    and response.term == self.currentTerm
                ):
                    self.votesReceived.add(peer_id)
                    if len(self.votesReceived) >= self.majority:
                        self.log.info(
                            "New Leader waiting for Old Leader Lease to timeout."
                        )
                        self.currentRole = TEMP_LEADER
                        return

    # ------------------------------------------------------------------
    # Log replication
    # ------------------------------------------------------------------

    def _append_local(self, command: str) -> None:
        """Append one entry to the local log and fsync it. Lock held."""
        entry = {"term": self.currentTerm, "command": command}
        self.entries.append(entry)
        self.storage.append_entries([entry])

    def _replicate_log(self, follower_id: int) -> None:
        """Push the follower's missing suffix, backing off on mismatch."""
        while True:
            with self._lock:
                if self.currentRole not in (LEADER, TEMP_LEADER):
                    return
                prefix_len = self.sentLength.get(follower_id, 0)
                suffix = self.entries[prefix_len:]
                message = raft_pb2.LogRequest(
                    leaderId=self.id,
                    term=self.currentTerm,
                    prefixLen=prefix_len,
                    prefixTerm=(
                        self.entries[prefix_len - 1]["term"] if prefix_len else 0
                    ),
                    leaderCommit=self.commitLength,
                    suffix=[
                        raft_pb2.Entry(term=e["term"], command=e["command"])
                        for e in suffix
                    ],
                    leaseInterval=config.LEASE_TIMEOUT,
                )

            try:
                response = self._stubs[follower_id].ProcessLog(
                    message, timeout=config.RPC_TIMEOUT
                )
            except grpc.RpcError:
                self.log.info(
                    "Error occurred while sending RPC to Node %d.", follower_id
                )
                return

            with self._lock:
                # A quorum of live followers renews the leader's lease.
                self.lease_acks += 1
                if self.lease_acks >= self.majority:
                    self.lease_timeout = time.time() + config.LEASE_TIMEOUT

                if response.term > self.currentTerm:
                    self.log.info("Node %d stepping down.", self.id)
                    self._set_term(response.term)
                    self._step_down()
                    return

                if response.term != self.currentTerm or self.currentRole != LEADER:
                    return

                if response.success and response.ack >= self.ackedLength.get(
                    follower_id, 0
                ):
                    self.sentLength[follower_id] = response.ack
                    self.ackedLength[follower_id] = response.ack
                    self._commit_log_entries()
                    return

                if self.sentLength.get(follower_id, 0) == 0:
                    return
                # Log mismatch: rewind one entry and try again.
                self.sentLength[follower_id] -= 1

    def _acks(self, length: int) -> int:
        return sum(
            1
            for peer_id in self.peer_ids
            if self.ackedLength.get(peer_id, 0) >= length
        )

    def _commit_log_entries(self) -> None:
        """Advance commitLength to the highest quorum-replicated prefix."""
        ready = [i for i in range(1, len(self.entries) + 1) if self._acks(i) >= self.majority]
        if not ready:
            return

        high = max(ready)
        # Only entries from the current term may be committed by counting
        # replicas (Raft §5.4.2); older ones commit via the no-op above.
        if high <= self.commitLength or self.entries[high - 1]["term"] != self.currentTerm:
            return

        for entry in self.entries[self.commitLength : high]:
            if entry["command"].split()[0] == "SET":
                self.log.info(
                    "Node %d (leader) committed the entry %s to the state machine.",
                    self.id,
                    entry,
                )
            self._apply(entry)

        self.commitLength = high
        self._persist_metadata()

    def ProcessLog(self, request, context):  # gRPC handler
        """Follower side of AppendEntries, including the lease grant."""
        with self._lock:
            self.election_timeout = self._next_election_timeout()
            self.lease_timeout = time.time() + request.leaseInterval

            if request.term > self.currentTerm:
                self._set_term(request.term)

            if request.term == self.currentTerm:
                self.currentRole = FOLLOWER
                self.currentLeader = request.leaderId

            log_ok = len(self.entries) >= request.prefixLen and (
                request.prefixLen == 0
                or self.entries[request.prefixLen - 1]["term"] == request.prefixTerm
            )

            if request.term == self.currentTerm and log_ok:
                self._append_entries(
                    request.prefixLen, request.leaderCommit, request.suffix
                )
                self.log.info(
                    "Node %d accepted AppendEntries RPC from %d.",
                    self.id,
                    request.leaderId,
                )
                return raft_pb2.LogResponse(
                    follower=self.id,
                    term=self.currentTerm,
                    ack=request.prefixLen + len(request.suffix),
                    success=True,
                )

            self.log.info(
                "Node %d rejected AppendEntries RPC from %d.",
                self.id,
                request.leaderId,
            )
            return raft_pb2.LogResponse(
                follower=self.id, term=self.currentTerm, ack=0, success=False
            )

    def _append_entries(self, prefix_len: int, leader_commit: int, suffix) -> None:
        """Truncate conflicting entries, append new ones, apply commits."""
        if suffix and len(self.entries) > prefix_len:
            index = min(len(self.entries), prefix_len + len(suffix)) - 1
            if self.entries[index]["term"] != suffix[index - prefix_len].term:
                self.entries = self.entries[:prefix_len]
                self.storage.rewrite_log(self.entries)

        if prefix_len + len(suffix) > len(self.entries):
            new = [
                {"term": suffix[i].term, "command": suffix[i].command}
                for i in range(len(self.entries) - prefix_len, len(suffix))
            ]
            self.entries.extend(new)
            self.storage.append_entries(new)

        if leader_commit > self.commitLength:
            for entry in self.entries[self.commitLength : leader_commit]:
                if entry["command"].split()[0] == "SET":
                    self.log.info(
                        "Node %d (follower) committed the entry %s to the state machine.",
                        self.id,
                        entry,
                    )
                self._apply(entry)
            self.commitLength = leader_commit
            self._persist_metadata()

    # ------------------------------------------------------------------
    # Client API
    # ------------------------------------------------------------------

    def ServeClient(self, request, context):  # gRPC handler
        """Handle ``SET k v`` / ``GET k``, redirecting if not the leader."""
        with self._lock:
            if self.currentRole != LEADER:
                return raft_pb2.ServeClientReply(
                    Data=f"Update Leader, LeaderId = {self.currentLeader}",
                    LeaderID=self.currentLeader or 0,
                    Success=False,
                )

            self.log.info(
                "Node %d (leader) received an %s request.", self.id, request.Request
            )
            parts = request.Request.split()
            operation = parts[0].upper() if parts else ""

            if operation == "SET" and len(parts) >= 3:
                self._append_local(request.Request)
                self.ackedLength[self.id] = len(self.entries)
                followers = [p for p in self.peer_ids if p != self.id]
            elif operation == "GET" and len(parts) >= 2:
                # Safe to read locally: the lease guarantees no other node
                # is serving as leader right now.
                return raft_pb2.ServeClientReply(
                    Data=self.db.get(parts[1], ""),
                    LeaderID=self.id,
                    Success=True,
                )
            else:
                return raft_pb2.ServeClientReply(
                    Data=f"Malformed request: {request.Request}",
                    LeaderID=self.id,
                    Success=False,
                )

        for peer_id in followers:
            self._replicate_log(peer_id)

        return raft_pb2.ServeClientReply(
            Data="Entry Updated", LeaderID=self.id, Success=True
        )


def serve() -> None:
    parser = argparse.ArgumentParser(description="Run one Raft node.")
    parser.add_argument("--id", type=int, required=True, help="node id, 1-based")
    parser.add_argument(
        "--port", type=int, help="listen port (default: from the peer list)"
    )
    parser.add_argument("--data-dir", help="default: logs_node_<id>/")
    args = parser.parse_args()

    peers = config.load_peers()
    if not 1 <= args.id <= len(peers):
        parser.error(f"--id must be between 1 and {len(peers)}")
    port = args.port or int(peers[args.id - 1].rsplit(":", 1)[1])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    raft_pb2_grpc.add_RaftServicer_to_server(
        RaftNode(args.id, peers, args.data_dir), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Node {args.id} listening on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped")


if __name__ == "__main__":
    serve()
