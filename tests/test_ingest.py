"""Tests for the ingestion pipeline."""

from pathlib import Path

from distributed_prompt import FileBackend, ShardIndex, ingest_file, ingest_string


def test_ingest_string_roundtrip(tmp_path):
    data = "abcdefghijklmnopqrstuvwxyz"
    index = ingest_string(data, tmp_path, shard_size=10)

    assert index.total_length == 26
    assert index.num_shards == 3
    assert index.shard_size == 10

    # Read back via FileBackend
    backend = FileBackend(tmp_path)
    assert backend.fetch_range(0, 26) == data


def test_ingest_file_roundtrip(tmp_path):
    # Create a source file
    src = tmp_path / "source.txt"
    data = "The quick brown fox jumps over the lazy dog. " * 100
    src.write_text(data, encoding="utf-8")

    out = tmp_path / "shards"
    index = ingest_file(src, out, shard_size=100)

    assert index.total_length == len(data)
    assert (out / "meta.json").exists()

    # Read back
    backend = FileBackend(out)
    assert backend.fetch_range(0, len(data)) == data


def test_ingest_creates_meta_json(tmp_path):
    data = "hello"
    ingest_string(data, tmp_path, shard_size=10)
    meta_path = tmp_path / "meta.json"
    assert meta_path.exists()
    index = ShardIndex.load(meta_path)
    assert index.total_length == 5
    assert index.num_shards == 1


def test_ingest_shard_files(tmp_path):
    data = "abcdefghij"
    ingest_string(data, tmp_path, shard_size=3)
    # Should create: 0000.txt, 0001.txt, 0002.txt, 0003.txt
    assert (tmp_path / "0000.txt").read_text() == "abc"
    assert (tmp_path / "0001.txt").read_text() == "def"
    assert (tmp_path / "0002.txt").read_text() == "ghi"
    assert (tmp_path / "0003.txt").read_text() == "j"


def test_ingest_empty_string(tmp_path):
    """Edge case: empty input."""
    data = ""
    index = ingest_string(data, tmp_path, shard_size=10)
    assert index.total_length == 0
    assert index.num_shards == 1  # ceil(0/10) = max(1, 0) = 1


def test_shard_index_save_load_roundtrip(tmp_path):
    data = "test data here"
    index = ingest_string(data, tmp_path, shard_size=5)
    reloaded = ShardIndex.load(tmp_path / "meta.json")
    assert reloaded.total_length == index.total_length
    assert reloaded.num_shards == index.num_shards
    assert len(reloaded.shards) == len(index.shards)
