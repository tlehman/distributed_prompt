# DistributedPrompt (for scaling up [RLMs](https://github.com/alexzhang13/rlm?tab=readme-ov-file#recursive-language-models-rlms))

<p align="center">
  <a href="https://github.com/tlehman/distributed_prompt/actions/workflows/style.yml">
    <img src="https://github.com/tlehman/distributed_prompt/actions/workflows/style.yml/badge.svg" alt="Style" />
  </a>
  <a href="https://github.com/tlehman/distributed_prompt/actions/workflows/test.yml">
    <img src="https://github.com/tlehman/distributed_prompt/actions/workflows/test.yml/badge.svg" alt="Test" />
  </a>
</p>

Horizontally scalable prompts for Recursive Language Models (RLMs).

`DistributedPrompt` is a drop-in replacement for Python's `str` that stores
prompt data across fixed-size shards on disk or S3.  Slicing
(`prompt[n:n+k]`) performs O(1) shard lookup — only the needed data is
fetched.  This lets RLMs operate on prompts with 100M+ characters without
holding them in memory.

## Quick start

```bash
uv sync --all-groups    # installs Python 3.12, dev & test deps
uv tool install -e .    # installs the dprompt CLI on your PATH
```

### Ingest a file

```bash
uv run dprompt ingest ../mega_prompt.md --output ./shards/ --shard-size 10_000_000
uv run dprompt info ./shards/
```

### Use from Python / REPL

```python
from distributed_prompt import DistributedPrompt, FileBackend

backend = FileBackend("./shards/")
prompt = DistributedPrompt(backend)

# O(1) shard lookup — only fetches the two shards covering this range
print(prompt[10_000_000:10_000_000 + 1000])

print(len(prompt))    # no I/O — reads from metadata
print(repr(prompt))   # DistributedPrompt(length=..., shards=..., shard_size=...)

# str-like protocol
"keyword" in prompt   # scans shard-by-shard with overlap
prompt.find("needle") # returns character offset or -1
```

### S3 / MinIO backend (WIP)

```python
from distributed_prompt.backends.s3_backend import S3Backend, ingest_to_s3

# Upload local shards to MinIO
ingest_to_s3("./shards/", bucket="prompts", prefix="corpus-v1",
             endpoint_url="http://minio:9000")

# Read from S3
backend = S3Backend(bucket="prompts", prefix="corpus-v1",
                    endpoint_url="http://minio:9000")
prompt = DistributedPrompt(backend)
print(prompt[0:100])
```

## Integration with RLMs

In Zhang & Khattab 2025's Recursive Language Model, the agent loop evaluates
Python in a REPL where `context` is a plain string.  Replace it with:

```python
# Before (fails for large prompts):
prompt = open("huge_file.txt").read()

# After:
from distributed_prompt import DistributedPrompt, FileBackend
prompt = DistributedPrompt(FileBackend("./shards/"))
# context[n:n+k] works identically — O(1) shard fetch
```

The RLM's generated code can slice `prompt` as usual.  The `DistributedPrompt`
object transparently fetches only the needed shards.  LM inference latency
(seconds) dominates shard fetch latency (~100ms for S3), so this is
effectively free.

## Running tests

```bash
uv run pytest tests/ -v
```

## References

- Zhang, Alex and Khattab, Omar (2025). "Recursive Language Models." [https://alexzhang13.github.io/blog/2025/rlm/](https://alexzhang13.github.io/blog/2025/rlm/)
