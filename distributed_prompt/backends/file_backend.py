"""File-system based shard backend."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from distributed_prompt.backends.base import Backend
from distributed_prompt.shard import ShardIndex


class FileBackend(Backend):
    """Backend that reads shards from local files.

    Shards are stored as ``shards_dir/NNNN.txt`` with an accompanying
    ``meta.json`` index file.
    """

    def __init__(self, shards_dir: str | Path, cache_size: int = 32) -> None:
        self.shards_dir = Path(shards_dir)
        self.index = ShardIndex.load(self.shards_dir / "meta.json")
        # Build a cached reader with the specified LRU size.
        self._read_shard = lru_cache(maxsize=cache_size)(self._read_shard_uncached)

    def _shard_path(self, shard_id: int) -> Path:
        return self.shards_dir / f"{shard_id:04d}.txt"

    def _read_shard_uncached(self, shard_id: int) -> str:
        return self._shard_path(shard_id).read_text(encoding="utf-8")

    def get_shard(self, shard_id: int) -> str:
        return self._read_shard(shard_id)

    def get_shard_slice(self, shard_id: int, offset: int, length: int) -> str:
        data = self._read_shard(shard_id)
        return data[offset : offset + length]
