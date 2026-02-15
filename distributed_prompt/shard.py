"""Shard metadata and index data structures."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ShardMeta:
    """Metadata for a single shard."""

    shard_id: int
    start_offset: int
    end_offset: int
    byte_length: int


@dataclass
class ShardIndex:
    """Index mapping character offsets to shards.

    Supports O(1) lookup of which shard(s) contain a given byte range.
    """

    total_length: int
    shard_size: int
    num_shards: int
    source_file: str
    shards: list[ShardMeta] = field(default_factory=list)

    def lookup(self, start: int, stop: int) -> list[int]:
        """Return shard IDs covering [start, stop). O(1) via integer division."""
        if start < 0:
            start = max(0, self.total_length + start)
        if stop < 0:
            stop = max(0, self.total_length + stop)
        start = min(start, self.total_length)
        stop = min(stop, self.total_length)
        if start >= stop:
            return []
        first = start // self.shard_size
        last = (stop - 1) // self.shard_size
        return list(range(first, last + 1))

    def save(self, path: str | Path) -> None:
        """Serialize index to JSON."""
        data = {
            "total_length": self.total_length,
            "shard_size": self.shard_size,
            "num_shards": self.num_shards,
            "source_file": self.source_file,
            "shards": [asdict(s) for s in self.shards],
        }
        Path(path).write_text(json.dumps(data, indent=2))

    @classmethod
    def load(cls, path: str | Path) -> ShardIndex:
        """Deserialize index from JSON."""
        data = json.loads(Path(path).read_text())
        shards = [ShardMeta(**s) for s in data["shards"]]
        return cls(
            total_length=data["total_length"],
            shard_size=data["shard_size"],
            num_shards=data["num_shards"],
            source_file=data["source_file"],
            shards=shards,
        )

    @classmethod
    def build(cls, total_length: int, shard_size: int, source_file: str = "") -> ShardIndex:
        """Build a ShardIndex from total length and shard size."""
        num_shards = max(1, math.ceil(total_length / shard_size))
        shards = []
        for i in range(num_shards):
            start = i * shard_size
            end = min(start + shard_size, total_length)
            shards.append(ShardMeta(
                shard_id=i,
                start_offset=start,
                end_offset=end,
                byte_length=end - start,
            ))
        return cls(
            total_length=total_length,
            shard_size=shard_size,
            num_shards=num_shards,
            source_file=source_file,
            shards=shards,
        )
