"""Tests for FileBackend."""

from distributed_prompt import FileBackend, ingest_string


def test_single_shard_read(tmp_path):
    data = "hello world"
    ingest_string(data, tmp_path, shard_size=100)
    backend = FileBackend(tmp_path)
    assert backend.fetch_range(0, 5) == "hello"
    assert backend.fetch_range(6, 11) == "world"


def test_cross_shard_read(tmp_path):
    data = "abcdefghij"  # 10 chars
    ingest_string(data, tmp_path, shard_size=3)
    backend = FileBackend(tmp_path)
    # Spans shards 0 (abc), 1 (def), 2 (ghi)
    assert backend.fetch_range(1, 8) == "bcdefgh"


def test_full_read(tmp_path):
    data = "the quick brown fox"
    ingest_string(data, tmp_path, shard_size=5)
    backend = FileBackend(tmp_path)
    assert backend.fetch_range(0, len(data)) == data


def test_empty_range(tmp_path):
    data = "something"
    ingest_string(data, tmp_path, shard_size=3)
    backend = FileBackend(tmp_path)
    assert backend.fetch_range(5, 5) == ""
    assert backend.fetch_range(5, 3) == ""


def test_cache_behavior(tmp_path):
    """Reading the same shard twice should hit the LRU cache."""
    data = "abcdefghij" * 10
    ingest_string(data, tmp_path, shard_size=10)
    backend = FileBackend(tmp_path, cache_size=4)

    # First read
    r1 = backend.fetch_range(0, 5)
    # Second read from same shard — should be cached
    r2 = backend.fetch_range(5, 10)
    assert r1 == "abcde"
    assert r2 == "fghij"

    info = backend._read_shard.cache_info()
    assert info.hits >= 1


def test_get_shard(tmp_path):
    data = "0123456789"
    ingest_string(data, tmp_path, shard_size=5)
    backend = FileBackend(tmp_path)
    assert backend.get_shard(0) == "01234"
    assert backend.get_shard(1) == "56789"


def test_get_shard_slice(tmp_path):
    data = "0123456789"
    ingest_string(data, tmp_path, shard_size=5)
    backend = FileBackend(tmp_path)
    assert backend.get_shard_slice(0, 2, 3) == "234"
    assert backend.get_shard_slice(1, 0, 2) == "56"
