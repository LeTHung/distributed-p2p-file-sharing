"""Streaming SHA-256 metadata using the team's JSON field names.

Usage: python -m common.metadata FILE --chunk-size 1048576 --output FILE.torrent.json
The output is JSON metadata for this project, not BitTorrent's bencoded format.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Any

HASH = re.compile(r"[0-9a-f]{64}\Z")
EMPTY_HASH = hashlib.sha256(b"").hexdigest()


def validate_metadata(metadata: dict[str, Any]) -> None:
    """Validate structure; only a peer with the bytes can verify content hashes."""
    if not isinstance(metadata.get("file_id"), str) or not HASH.fullmatch(metadata["file_id"]):
        raise ValueError("file_id must be a lowercase SHA-256 hex string")
    name = metadata.get("file_name")
    if not isinstance(name, str) or not name.strip() or len(name) > 1024 or "\0" in name:
        raise ValueError("file_name must be a nonempty string of at most 1024 characters")
    for field, minimum, maximum in (("file_size", 0, 2**63 - 1),
                                    ("chunk_size", 1, 2**31 - 1),
                                    ("total_chunks", 0, 2**31 - 1)):
        value = metadata.get(field)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"{field} must be an integer between {minimum} and {maximum}")
    expected = (metadata["file_size"] + metadata["chunk_size"] - 1) // metadata["chunk_size"]
    if metadata["total_chunks"] != expected:
        raise ValueError("total_chunks does not match file_size and chunk_size")
    chunks = metadata.get("chunks")
    if not isinstance(chunks, list) or len(chunks) != expected:
        raise ValueError("chunks must contain exactly total_chunks entries")
    indexes = set()
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise ValueError("each chunk must be an object")
        index, digest = chunk.get("chunk_index"), chunk.get("chunk_hash")
        if type(index) is not int or not 0 <= index < expected or index in indexes:
            raise ValueError("chunk indexes must be unique and cover 0 through total_chunks - 1")
        if not isinstance(digest, str) or not HASH.fullmatch(digest):
            raise ValueError("chunk_hash must be a lowercase SHA-256 hex string")
        indexes.add(index)
    if expected == 0 and metadata["file_id"] != EMPTY_HASH:
        raise ValueError("empty file_id must be SHA-256 of empty bytes")


def build_metadata(path: str | Path, chunk_size: int = 1024 * 1024) -> dict[str, Any]:
    if type(chunk_size) is not int or not 1 <= chunk_size <= 2**31 - 1:
        raise ValueError("chunk_size must be a positive PostgreSQL integer")
    source_path = Path(path)
    digest = hashlib.sha256()
    chunks = []
    size = 0
    with source_path.open("rb") as source:
        while data := source.read(chunk_size):
            digest.update(data)
            size += len(data)
            chunks.append({"chunk_index": len(chunks), "chunk_hash": hashlib.sha256(data).hexdigest()})
    metadata = {"file_id": digest.hexdigest(), "file_name": source_path.name,
                "file_size": size, "chunk_size": chunk_size, "total_chunks": len(chunks),
                "chunks": chunks}
    validate_metadata(metadata)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--chunk-size", type=int, default=1024 * 1024)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        encoded = json.dumps(build_metadata(args.file, args.chunk_size), ensure_ascii=False, indent=2)
        if args.output:
            with args.output.open("x", encoding="utf-8") as output:
                output.write(encoded + "\n")
        else:
            print(encoded)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Metadata error: {error}\n")


if __name__ == "__main__":
    main()
