"""DistributedPrompt — a drop-in, lazily-fetched replacement for str."""

from __future__ import annotations

from distributed_prompt.backends.base import Backend


class DistributedPrompt:
    """A str-like object backed by sharded storage.

    Supports ``prompt[n:n+k]`` with O(1) shard lookup — only the needed
    data is fetched from the backend (disk or S3).

    This is *not* a ``str`` subclass.  Subclassing ``str`` forces full
    materialisation at construction time, defeating the purpose.
    """

    def __init__(self, backend: Backend) -> None:
        self._backend = backend

    # -- core access -----------------------------------------------------------

    @property
    def _index(self):
        return self._backend.index

    def __len__(self) -> int:
        return self._index.total_length

    def __getitem__(self, key: int | slice) -> str:
        if isinstance(key, int):
            if key < 0:
                key = len(self) + key
            if key < 0 or key >= len(self):
                raise IndexError("index out of range")
            return self._backend.fetch_range(key, key + 1)
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            if step != 1:
                # Fall back to character-by-character for non-unit steps.
                return "".join(self._backend.fetch_range(i, i + 1) for i in range(start, stop, step))
            return self._backend.fetch_range(start, stop)
        raise TypeError(f"indices must be integers or slices, not {type(key).__name__}")

    # -- string protocol -------------------------------------------------------

    def __str__(self) -> str:
        n = len(self)
        if n <= 200:
            return self._backend.fetch_range(0, n)
        head = self._backend.fetch_range(0, 80)
        tail = self._backend.fetch_range(n - 80, n)
        return f"{head}...({n - 160} chars omitted)...{tail}"

    def __repr__(self) -> str:
        idx = self._index
        return (
            f"DistributedPrompt(length={idx.total_length:,}, "
            f"shards={idx.num_shards}, shard_size={idx.shard_size:,})"
        )

    def __contains__(self, item: str) -> bool:  # type: ignore[override]
        # Scan shard by shard with overlap to catch cross-boundary matches.
        overlap = len(item) - 1 if len(item) > 1 else 0
        for shard in self._index.shards:
            start = max(0, shard.start_offset - overlap)
            end = shard.end_offset
            chunk = self._backend.fetch_range(start, end)
            if item in chunk:
                return True
        return False

    def find(self, sub: str, start: int = 0, end: int | None = None) -> int:
        if end is None:
            end = len(self)
        overlap = len(sub) - 1 if len(sub) > 1 else 0
        shard_ids = self._index.lookup(start, end)
        for sid in shard_ids:
            meta = self._index.shards[sid]
            fetch_start = max(start, meta.start_offset - overlap) if sid != shard_ids[0] else start
            fetch_end = min(end, meta.end_offset)
            chunk = self._backend.fetch_range(fetch_start, fetch_end)
            pos = chunk.find(sub)
            if pos != -1:
                return fetch_start + pos
        return -1

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __format__(self, format_spec: str) -> str:
        return format(str(self), format_spec)

    def __add__(self, other: str) -> str:
        return str(self) + other

    def __radd__(self, other: str) -> str:
        return other + str(self)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            if len(self) != len(other):
                return False
            # Compare shard by shard to avoid full materialisation for mismatches.
            for meta in self._index.shards:
                chunk = self._backend.fetch_range(meta.start_offset, meta.end_offset)
                if chunk != other[meta.start_offset : meta.end_offset]:
                    return False
            return True
        if isinstance(other, DistributedPrompt):
            if len(self) != len(other):
                return False
            for meta in self._index.shards:
                a = self._backend.fetch_range(meta.start_offset, meta.end_offset)
                b = other[meta.start_offset : meta.end_offset]
                if a != b:
                    return False
            return True
        return NotImplemented

    def __hash__(self):
        raise TypeError("unhashable type: 'DistributedPrompt'")

    def __bool__(self) -> bool:
        return len(self) > 0
