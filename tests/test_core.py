"""Tests for DistributedPrompt (core.py)."""

import pytest

from distributed_prompt import DistributedPrompt, FileBackend, ingest_string


@pytest.fixture
def prompt(tmp_path):
    """Create a DistributedPrompt from a known string, shard_size=10."""
    data = "abcdefghijklmnopqrstuvwxyz" * 4  # 104 chars
    ingest_string(data, tmp_path, shard_size=10)
    backend = FileBackend(tmp_path)
    return DistributedPrompt(backend), data


def test_len(prompt):
    dp, data = prompt
    assert len(dp) == len(data)


def test_getitem_single(prompt):
    dp, data = prompt
    assert dp[0] == data[0]
    assert dp[5] == data[5]
    assert dp[-1] == data[-1]


def test_getitem_slice(prompt):
    dp, data = prompt
    assert dp[0:10] == data[0:10]
    assert dp[5:15] == data[5:15]  # cross-shard
    assert dp[100:104] == data[100:104]  # last shard partial
    assert dp[:10] == data[:10]
    assert dp[-5:] == data[-5:]


def test_getitem_step(prompt):
    dp, data = prompt
    assert dp[0:20:2] == data[0:20:2]


def test_getitem_out_of_range(prompt):
    dp, _ = prompt
    with pytest.raises(IndexError):
        dp[200]


def test_str_short():
    """Short prompts are returned in full."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        data = "hello world"
        ingest_string(data, td, shard_size=100)
        dp = DistributedPrompt(FileBackend(td))
        assert str(dp) == data


def test_str_long(tmp_path):
    """Long prompts are truncated."""
    data = "x" * 1000
    ingest_string(data, tmp_path, shard_size=100)
    dp = DistributedPrompt(FileBackend(tmp_path))
    s = str(dp)
    assert "omitted" in s
    assert len(s) < 1000


def test_repr(prompt):
    dp, _ = prompt
    r = repr(dp)
    assert "DistributedPrompt" in r
    assert "104" in r


def test_contains(prompt):
    dp, _ = prompt
    assert "abc" in dp
    assert "xyz" in dp
    assert "zzz" not in dp


def test_contains_cross_boundary(tmp_path):
    """Substring spanning a shard boundary is found."""
    data = "aaabbbcccddd"
    ingest_string(data, tmp_path, shard_size=3)
    dp = DistributedPrompt(FileBackend(tmp_path))
    assert "bbbccc" in dp  # spans shards 1 and 2


def test_find(prompt):
    dp, data = prompt
    assert dp.find("klm") == data.find("klm")
    assert dp.find("zzz") == -1


def test_eq_str(prompt):
    dp, data = prompt
    assert dp == data


def test_eq_different(prompt):
    dp, data = prompt
    assert not (dp == "wrong")


def test_bool(prompt):
    dp, _ = prompt
    assert bool(dp)


def test_add(prompt):
    dp, _ = prompt
    result = dp + "!!!"
    assert result.endswith("!!!")


def test_radd(prompt):
    dp, _ = prompt
    result = ">>>" + dp
    assert result.startswith(">>>")


def test_iter(tmp_path):
    data = "abcde"
    ingest_string(data, tmp_path, shard_size=2)
    dp = DistributedPrompt(FileBackend(tmp_path))
    assert list(dp) == list(data)


def test_format(prompt):
    dp, _ = prompt
    assert f"{dp}" == str(dp)
