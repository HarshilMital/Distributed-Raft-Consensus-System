"""Crash-recoverable, on-disk state for a single Raft node.

Every node owns a directory (``logs_node_<id>/`` by default) holding three
files:

``metadata.txt``
    One line: ``<currentTerm> <votedFor> <commitLength>``. Rewritten
    whenever any of the three changes.
``log.txt``
    One replicated entry per line: ``<command> <term>``. The command may
    contain spaces, so the term is parsed off the right hand side.
``dump.txt``
    Human readable event log, written by the node's logger.

Keeping the I/O here means the consensus code in :mod:`raft_node` never
touches the filesystem directly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional


class NodeStorage:
    """Reads and writes the durable state of one node."""

    def __init__(self, node_id: int, data_dir: Optional[str] = None) -> None:
        self.dir = Path(data_dir or f"logs_node_{node_id}")
        self.dir.mkdir(parents=True, exist_ok=True)
        self.metadata_path = self.dir / "metadata.txt"
        self.log_path = self.dir / "log.txt"
        self.dump_path = self.dir / "dump.txt"

    # -- metadata ---------------------------------------------------------

    def load_metadata(self) -> tuple[int, Optional[int], int]:
        """Return ``(currentTerm, votedFor, commitLength)``, or defaults."""
        if not self.metadata_path.exists():
            return 0, None, 0

        term, voted_for, commit_length = self.metadata_path.read_text().split()
        return (
            int(term),
            None if voted_for == "None" else int(voted_for),
            int(commit_length),
        )

    def save_metadata(
        self, current_term: int, voted_for: Optional[int], commit_length: int
    ) -> None:
        self.metadata_path.write_text(f"{current_term} {voted_for} {commit_length}")

    # -- replicated log ---------------------------------------------------

    def load_log(self) -> list[dict]:
        """Return the persisted log as ``[{'term': int, 'command': str}, ...]``."""
        if not self.log_path.exists():
            return []

        entries = []
        for line in self.log_path.read_text().splitlines():
            if not line:
                continue
            command, term = line.rsplit(" ", 1)
            entries.append({"term": int(term), "command": command})
        return entries

    def append_entries(self, entries: list[dict]) -> None:
        with self.log_path.open("a") as f:
            for entry in entries:
                f.write(f"{entry['command']} {entry['term']}\n")

    def rewrite_log(self, entries: list[dict]) -> None:
        """Replace the whole log file, used when a follower truncates."""
        with self.log_path.open("w") as f:
            for entry in entries:
                f.write(f"{entry['command']} {entry['term']}\n")
