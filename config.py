"""Cluster configuration.

Peer addresses are resolved in this order:

1. the ``RAFT_PEERS`` environment variable — a comma separated list of
   ``host:port`` entries, where the Nth entry is the address of node N
   (node ids are 1-based);
2. a ``cluster.json`` file next to this module, containing the same list
   under a ``"peers"`` key;
3. a five node cluster on localhost, ports 50051-50055.

This keeps the code deployment agnostic: the same image runs under
docker-compose, on a laptop, or on five cloud VMs.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

DEFAULT_PEERS = [f"127.0.0.1:{50051 + i}" for i in range(5)]

# How long a leader's lease lasts, in seconds. While the lease is valid the
# leader may answer reads locally; when it cannot renew it, it steps down.
LEASE_TIMEOUT = 10

# Deadline applied to every outbound RPC. Must stay well below the election
# timeout so that an unreachable peer cannot stall the main loop.
RPC_TIMEOUT = 1.0

# Bounds of the randomised election timeout, in seconds.
ELECTION_TIMEOUT_MIN = 5
ELECTION_TIMEOUT_MAX = 10

# Seconds between heartbeats sent by the leader.
HEARTBEAT_INTERVAL = 1

_CONFIG_FILE = Path(__file__).with_name("cluster.json")


def load_peers() -> list[str]:
    """Return the ordered list of peer addresses; index i is node i + 1."""
    env = os.environ.get("RAFT_PEERS")
    if env:
        return [addr.strip() for addr in env.split(",") if addr.strip()]

    if _CONFIG_FILE.exists():
        with _CONFIG_FILE.open() as f:
            return list(json.load(f)["peers"])

    return list(DEFAULT_PEERS)


def majority(cluster_size: int) -> int:
    """Smallest number of nodes that forms a quorum."""
    return cluster_size // 2 + 1
