"""S3/MinIO-compatible shard backend."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from distributed_prompt.backends.base import Backend
from distributed_prompt.shard import ShardIndex

try:
    import boto3
except ImportError:
    boto3 = None  # type: ignore[assignment]


class S3Backend(Backend):
    """Backend that reads shards from an S3-compatible object store.

    Works with AWS S3 and MinIO (via ``endpoint_url``).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        cache_size: int = 32,
        **boto_kwargs: Any,
    ) -> None:
        if boto3 is None:
            raise ImportError("boto3 is required for S3Backend: pip install boto3")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = boto3.client("s3", endpoint_url=endpoint_url, **boto_kwargs)
        self.index = self._load_index()
        self._read_shard = lru_cache(maxsize=cache_size)(self._read_shard_uncached)

    def _key(self, name: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{name}"
        return name

    def _load_index(self) -> ShardIndex:
        resp = self._client.get_object(Bucket=self.bucket, Key=self._key("meta.json"))
        data = json.loads(resp["Body"].read().decode("utf-8"))
        from distributed_prompt.shard import ShardMeta

        shards = [ShardMeta(**s) for s in data["shards"]]
        return ShardIndex(
            total_length=data["total_length"],
            shard_size=data["shard_size"],
            num_shards=data["num_shards"],
            source_file=data["source_file"],
            shards=shards,
        )

    def _read_shard_uncached(self, shard_id: int) -> str:
        key = self._key(f"{shard_id:04d}.txt")
        resp = self._client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read().decode("utf-8")

    def get_shard(self, shard_id: int) -> str:
        return self._read_shard(shard_id)

    def get_shard_slice(self, shard_id: int, offset: int, length: int) -> str:
        data = self._read_shard(shard_id)
        return data[offset : offset + length]


def ingest_to_s3(
    shards_dir: str,
    bucket: str,
    prefix: str = "",
    endpoint_url: str | None = None,
    **boto_kwargs: Any,
) -> None:
    """Upload a local shard directory to an S3 bucket."""
    if boto3 is None:
        raise ImportError("boto3 is required: pip install boto3")
    from pathlib import Path

    client = boto3.client("s3", endpoint_url=endpoint_url, **boto_kwargs)
    shards_path = Path(shards_dir)
    prefix = prefix.strip("/")

    for f in sorted(shards_path.iterdir()):
        if f.is_file():
            key = f"{prefix}/{f.name}" if prefix else f.name
            client.upload_file(str(f), bucket, key)
