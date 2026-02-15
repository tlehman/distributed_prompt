"""Abstract backend interface for shard storage."""

from __future__ import annotations

from abc import ABC, abstractmethod

from distributed_prompt.shard import ShardIndex


class Backend(ABC):
    """Abstract base class for shard storage backends."""

    index: ShardIndex

    @abstractmethod
    def get_shard(self, shard_id: int) -> str:
        """Fetch the full contents of a shard."""
        ...

    @abstractmethod
    def get_shard_slice(self, shard_id: int, offset: int, length: int) -> str:
        """Fetch a slice within a shard (offset relative to shard start)."""
        ...

    def fetch_range(self, start: int, stop: int) -> str:
        """Fetch characters in [start, stop) across shards.

        This is the core algorithm shared by all backends:
        1. Compute which shards are needed (O(1) lookup).
        2. For each shard, compute the local offset and length.
        3. Fetch slices and concatenate.
        """
        total = self.index.total_length
        if start < 0:
            start = max(0, total + start)
        if stop < 0:
            stop = max(0, total + stop)
        start = min(start, total)
        stop = min(stop, total)
        if start >= stop:
            return ""

        shard_ids = self.index.lookup(start, stop)
        parts: list[str] = []
        for sid in shard_ids:
            meta = self.index.shards[sid]
            local_start = max(0, start - meta.start_offset)
            local_end = min(meta.byte_length, stop - meta.start_offset)
            length = local_end - local_start
            if length <= 0:
                continue
            parts.append(self.get_shard_slice(sid, local_start, length))
        return "".join(parts)
