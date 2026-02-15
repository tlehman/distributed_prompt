"""CLI entry point for distributed_prompt."""

from __future__ import annotations

import argparse
import sys

from distributed_prompt.ingest import DEFAULT_SHARD_SIZE, ingest_file
from distributed_prompt.shard import ShardIndex


def cmd_ingest(args: argparse.Namespace) -> None:
    import tiktoken

    path = args.file
    output = args.output
    shard_size = args.shard_size
    print(f"Ingesting {path} → {output} (shard_size={shard_size:,})")
    index = ingest_file(path, output, shard_size)

    enc = tiktoken.get_encoding("cl100k_base")
    with open(path, encoding="utf-8") as f:
        num_tokens = len(enc.encode(f.read()))

    print(
        f"Done: {index.num_shards} shards, {index.total_length:,} characters, {num_tokens:,} tokens"
    )


def cmd_slice(args: argparse.Namespace) -> None:
    from distributed_prompt.backends.file_backend import FileBackend

    backend = FileBackend(args.shards_dir)
    text = backend.fetch_range(args.start, args.end)
    sys.stdout.write(text)


def cmd_info(args: argparse.Namespace) -> None:
    index = ShardIndex.load(f"{args.shards_dir}/meta.json")
    print(f"Source:      {index.source_file}")
    print(f"Total chars: {index.total_length:,}")
    print(f"Shard size:  {index.shard_size:,}")
    print(f"Num shards:  {index.num_shards}")
    if index.shards:
        last = index.shards[-1]
        print(f"Last shard:  {last.byte_length:,} chars (id={last.shard_id})")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="distributed_prompt",
        description="Distributed prompt ingestion and inspection.",
    )
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser("ingest", help="Ingest a file into shards")
    p_ingest.add_argument("file", help="Path to input file")
    p_ingest.add_argument("--output", "-o", required=True, help="Output directory for shards")
    p_ingest.add_argument(
        "--shard-size",
        "-s",
        type=int,
        default=DEFAULT_SHARD_SIZE,
        help=f"Shard size in characters (default: {DEFAULT_SHARD_SIZE:,})",
    )

    p_slice = sub.add_parser("slice", help="Read a character range from sharded prompt")
    p_slice.add_argument("shards_dir", help="Path to shards directory")
    p_slice.add_argument("--start", type=int, default=0, help="Start character offset (default: 0)")
    p_slice.add_argument("--end", type=int, required=True, help="End character offset (exclusive)")

    p_info = sub.add_parser("info", help="Show info about a shard directory")
    p_info.add_argument("shards_dir", help="Path to shards directory")

    args = parser.parse_args(argv)
    if args.command == "ingest":
        cmd_ingest(args)
    elif args.command == "slice":
        cmd_slice(args)
    elif args.command == "info":
        cmd_info(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
