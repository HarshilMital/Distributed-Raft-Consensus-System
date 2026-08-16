"""Command line client for the Raft key-value store.

The client talks to any node. If that node is not the leader it replies
with the leader's id, and the client retries there; if it is unreachable
the client round-robins through the cluster until it finds the leader.

    python client.py                  # interactive shell
    python client.py SET name alice   # one-shot command
    python client.py GET name
"""

from __future__ import annotations

import sys
from typing import Optional

import grpc

import config
import raft_pb2
import raft_pb2_grpc


class RaftClient:
    """Leader-aware client: discovers and follows the current leader."""

    def __init__(self, peers: list[str], max_attempts: int = 10) -> None:
        self.peers = peers
        self.max_attempts = max_attempts
        self.leader_id = 1
        self._stubs: dict[int, raft_pb2_grpc.RaftStub] = {}

    def _stub(self, node_id: int) -> raft_pb2_grpc.RaftStub:
        if node_id not in self._stubs:
            channel = grpc.insecure_channel(self.peers[node_id - 1])
            self._stubs[node_id] = raft_pb2_grpc.RaftStub(channel)
        return self._stubs[node_id]

    def _next_node(self) -> None:
        self.leader_id = self.leader_id % len(self.peers) + 1

    def send(self, command: str) -> Optional[str]:
        """Run one command, retrying until a leader accepts it."""
        request = raft_pb2.ServeClientArgs(Request=command)

        for _ in range(self.max_attempts):
            target = self.leader_id
            try:
                response = self._stub(target).ServeClient(
                    request, timeout=config.RPC_TIMEOUT * 5
                )
            except grpc.RpcError:
                print(f"Node {target} unreachable, trying the next node.")
                self._next_node()
                continue

            if response.Success:
                return response.Data

            # Not the leader: follow the redirect when we get one.
            if response.LeaderID and response.LeaderID != target:
                print(f"Node {target} is not the leader, redirecting to "
                      f"{response.LeaderID}.")
                self.leader_id = response.LeaderID
            else:
                print(f"Node {target} does not know the leader yet, "
                      f"trying the next node.")
                self._next_node()

        print(f"Giving up after {self.max_attempts} attempts.")
        return None

    def set(self, key: str, value: str) -> None:
        if self.send(f"SET {key} {value}") is not None:
            print("Entry updated.")

    def get(self, key: str) -> None:
        value = self.send(f"GET {key}")
        if value is not None:
            print(f"Value: {value!r}" if value else "Key not found.")


def repl(client: RaftClient) -> None:
    print("Commands: SET <key> <value> | GET <key> | exit")
    while True:
        try:
            parts = input("raft> ").split()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not parts:
            continue
        operation = parts[0].upper()

        if operation in ("EXIT", "QUIT"):
            return
        if operation == "SET" and len(parts) == 3:
            client.set(parts[1], parts[2])
        elif operation == "GET" and len(parts) == 2:
            client.get(parts[1])
        else:
            print("Usage: SET <key> <value> | GET <key> | exit")


def main() -> None:
    client = RaftClient(config.load_peers())
    args = sys.argv[1:]

    if not args:
        repl(client)
    elif args[0].upper() == "SET" and len(args) == 3:
        client.set(args[1], args[2])
    elif args[0].upper() == "GET" and len(args) == 2:
        client.get(args[1])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
