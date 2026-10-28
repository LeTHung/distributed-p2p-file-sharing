"""Controlled integration probe; this is not Student 3's downloader."""

import argparse
import hashlib
import json
import socket
import uuid

from common.framing import recv_exact, recv_json, send_json


def request(address, payload):
    request_id = str(uuid.uuid4())
    with socket.create_connection(address, timeout=5) as sock:
        send_json(sock, {**payload, "request_id": request_id})
        response = recv_json(sock)
    assert response["request_id"] == request_id and response["ok"], response
    return response


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker-port", required=True, type=int)
    parser.add_argument("--file-id", required=True)
    args = parser.parse_args()
    metadata = request(("127.0.0.1", args.tracker_port), {"type": "get_peers", "file_id": args.file_id})
    digest = hashlib.sha256()
    size = 0
    for chunk in metadata["chunks"]:
        index = chunk["chunk_index"]
        peer = next(p for p in metadata["peers"] if index in p["chunk_indexes"])
        request_id = str(uuid.uuid4())
        with socket.create_connection((peer["host"], peer["port"]), timeout=5) as sock:
            send_json(sock, {"type": "get_chunk", "request_id": request_id,
                             "file_id": args.file_id, "chunk_index": index})
            header = recv_json(sock)
            assert header["ok"] and header["request_id"] == request_id, header
            assert header["file_id"] == args.file_id and header["chunk_index"] == index, header
            expected = min(metadata["chunk_size"], metadata["file_size"] - index * metadata["chunk_size"])
            assert header["data_length"] == expected, header
            data = recv_exact(sock, expected)
        assert hashlib.sha256(data).hexdigest() == chunk["chunk_hash"], "Chunk hash mismatch"
        digest.update(data)
        size += len(data)
    assert size == metadata["file_size"] and digest.hexdigest() == args.file_id, "File hash mismatch"
    print(json.dumps({"verified": True, "bytes": size, "chunks": metadata["total_chunks"],
                      "file_id": digest.hexdigest(), "source": "real uploader; controlled probe"}))


if __name__ == "__main__":
    main()
