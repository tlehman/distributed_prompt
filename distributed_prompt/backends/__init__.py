"""Shard storage backends."""

from distributed_prompt.backends.file_backend import FileBackend

__all__ = ["FileBackend"]

# S3Backend imported lazily to avoid hard boto3 dependency.
