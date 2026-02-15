"""Ingestion pipeline: file/string → shards + meta.json."""

from __future__ import annotations

import math
from pathlib import Path

from distributed_prompt.shard import ShardIndex, ShardMeta

DEFAULT_SHARD_SIZE = 1_000_000  # 1 MB (in characters)


def ingest_file(
    path: str | Path,
    output_dir: str | Path,
    shard_size: int = DEFAULT_SHARD_SIZE,
) -> ShardIndex:
    """Stream a file into fixed-size shard files + meta.json.

    Never holds more than one shard in memory at a time.
    """
    path = Path(path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    total_length = path.stat().st_size  # byte length ≈ char length for UTF-8 ASCII
    num_shards = max(1, math.ceil(total_length / shard_size))
    shards: list[ShardMeta] = []

    with open(path, encoding="utf-8") as f:
        for i in range(num_shards):
            chunk = f.read(shard_size)
            if not chunk:
                break
            start = i * shard_size
            end = start + len(chunk)
            shard_path = output_dir / f"{i:04d}.txt"
            shard_path.write_text(chunk, encoding="utf-8")
            shards.append(
                ShardMeta(
                    shard_id=i,
                    start_offset=start,
                    end_offset=end,
                    byte_length=len(chunk),
                )
            )

    actual_length = shards[-1].end_offset if shards else 0
    index = ShardIndex(
        total_length=actual_length,
        shard_size=shard_size,
        num_shards=len(shards),
        source_file=str(path),
        shards=shards,
    )
    index.save(output_dir / "meta.json")
    return index


def ingest_string(
    data: str,
    output_dir: str | Path,
    shard_size: int = DEFAULT_SHARD_SIZE,
) -> ShardIndex:
    """Ingest a Python string into shards. Useful for testing."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    total_length = len(data)
    num_shards = max(1, math.ceil(total_length / shard_size))
    shards: list[ShardMeta] = []

    for i in range(num_shards):
        start = i * shard_size
        end = min(start + shard_size, total_length)
        chunk = data[start:end]
        shard_path = output_dir / f"{i:04d}.txt"
        shard_path.write_text(chunk, encoding="utf-8")
        shards.append(
            ShardMeta(
                shard_id=i,
                start_offset=start,
                end_offset=end,
                byte_length=len(chunk),
            )
        )

    index = ShardIndex(
        total_length=total_length,
        shard_size=shard_size,
        num_shards=len(shards),
        source_file="<string>",
        shards=shards,
    )
    index.save(output_dir / "meta.json")
    return index
