"""Week-one Seeder: python -m peer.uploader --help."""

from __future__ import annotations

import argparse
import hashlib
import logging
from pathlib import Path
import socket
import socketserver
import uuid

from common.framing import ProtocolError, recv_json, send_json

LOG = logging.getLogger(__name__)


class SharedFile:
    """Index a file without loading the whole file into memory."""

    def __init__(self, path: str | Path, chunk_size: int = 1024 * 1024):
        if type(chunk_size) is not int or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        self.path = Path(path).resolve(strict=True)
        self.chunk_size = chunk_size
        digest = hashlib.sha256()
        self.chunks = []
        self.file_size = 0
        with self.path.open("rb") as source:
            while data := source.read(chunk_size):
                digest.update(data)
                self.file_size += len(data)
                self.chunks.append({"chunk_index": len(self.chunks),
                                    "chunk_hash": hashlib.sha256(data).hexdigest()})
        if not self.chunks:
            raise ValueError("Empty files are not supported by the current database schema")
        self.file_id = digest.hexdigest()

    def metadata(self) -> dict:
        return {"file_id": self.file_id, "file_name": self.path.name,
                "file_size": self.file_size, "chunk_size": self.chunk_size,
                "total_chunks": len(self.chunks), "chunks": self.chunks}

    def read_chunk(self, index: int) -> bytes:
        if type(index) is not int or not 0 <= index < len(self.chunks):
            raise IndexError("Chunk index is outside this file")
        with self.path.open("rb") as source:
            source.seek(index * self.chunk_size)
            data = source.read(self.chunk_size)
        if hashlib.sha256(data).hexdigest() != self.chunks[index]["chunk_hash"]:
            raise OSError("Source file changed after indexing; restart the Seeder")
        return data


def tracker_request(address: tuple[str, int], payload: dict, timeout: float) -> dict:
    request = {**payload, "request_id": str(uuid.uuid4())}
    try:
        with socket.create_connection(address, timeout=timeout) as connection:
            send_json(connection, request)
            response = recv_json(connection)
        if response.get("request_id") != request["request_id"]:
            raise ProtocolError("Tracker response request_id does not match")
        if response.get("ok") is not True:
            raise ProtocolError(f"Tracker rejected {payload['type']}: {response}")
        return response
    except (OSError, EOFError, ProtocolError):
        LOG.exception("Tracker request failed request_id=%s peer_id=%s",
                      request["request_id"], payload.get("peer_id"))
        raise


class ChunkHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(self.server.timeout_seconds)
        request_id = None
        try:
            request = recv_json(self.request)
            request_id = request.get("request_id")
            if (request.get("type") != "get_chunk" or
                    not isinstance(request_id, str) or not request_id or
                    not isinstance(request.get("file_id"), str) or
                    type(request.get("chunk_index")) is not int):
                self.error(request_id, "INVALID_REQUEST", "Expected get_chunk with request_id, file_id, chunk_index")
                return
            shared = self.server.shared_file
            if request["file_id"] != shared.file_id:
                self.error(request_id, "FILE_NOT_FOUND", "File is not shared by this Seeder")
                return
            try:
                data = shared.read_chunk(request["chunk_index"])
            except IndexError:
                self.error(request_id, "CHUNK_NOT_FOUND", "Chunk index is outside this file")
                return
            except OSError:
                LOG.exception("Chunk unavailable request_id=%s", request_id)
                self.error(request_id, "CHUNK_UNAVAILABLE", "Source file is unavailable or changed")
                return
            send_json(self.request, {"request_id": request_id, "ok": True,
                                    "file_id": shared.file_id, "chunk_index": request["chunk_index"],
                                    "data_length": len(data)})
            self.request.sendall(data)
            LOG.info("Sent chunk=%s bytes=%s request_id=%s", request["chunk_index"], len(data), request_id)
        except ProtocolError as error:
            LOG.warning("Invalid frame request_id=%s: %s", request_id, error)
            try:
                self.error(request_id, "INVALID_REQUEST", str(error))
            except OSError:
                pass
        except (OSError, EOFError):
            LOG.warning("Peer connection failed request_id=%s", request_id, exc_info=True)

    def error(self, request_id, code, message):
        send_json(self.request, {"request_id": request_id, "ok": False,
                                "error_code": code, "message": message})


class UploaderServer(socketserver.TCPServer):
    """Sequential server for week one; worker pool belongs to week two."""

    allow_reuse_address = True

    def __init__(self, address, shared_file: SharedFile, timeout_seconds: float = 5):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.shared_file = shared_file
        self.timeout_seconds = timeout_seconds
        super().__init__(address, ChunkHandler)


def main():
    from common.config import load_settings

    settings = load_settings()
    parser = argparse.ArgumentParser(description="Share one file through the week-one Seeder")
    parser.add_argument("file", type=Path)
    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--host", default="127.0.0.1", help="Listen address")
    parser.add_argument("--advertise-host", help="Address reachable by other peers (required for wildcard binds)")
    parser.add_argument("--port", type=int, default=7001)
    parser.add_argument("--chunk-size", type=int, default=1024 * 1024)
    parser.add_argument("--tracker-host", default=settings.tracker_host)
    parser.add_argument("--tracker-port", type=int, default=settings.tracker_port)
    parser.add_argument("--skip-tracker", action="store_true", help="Local peer testing only")
    args = parser.parse_args()
    if args.host in ("0.0.0.0", "::") and not args.advertise_host:
        parser.error("Wildcard bind requires --advertise-host")
    if not 1 <= args.port <= 65535 or not 1 <= args.tracker_port <= 65535:
        parser.error("Ports must be between 1 and 65535")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        shared = SharedFile(args.file, args.chunk_size)
        with UploaderServer((args.host, args.port), shared, settings.socket_timeout_seconds) as server:
            if not args.skip_tracker:
                tracker = (args.tracker_host, args.tracker_port)
                tracker_request(tracker, {"type": "register_peer", "peer_id": args.peer_id,
                                          "host": args.advertise_host or args.host, "port": args.port},
                                settings.socket_timeout_seconds)
                tracker_request(tracker, {"type": "register_file", **shared.metadata()},
                                settings.socket_timeout_seconds)
                tracker_request(tracker, {"type": "announce_chunks", "peer_id": args.peer_id,
                                          "file_id": shared.file_id,
                                          "chunk_indexes": list(range(len(shared.chunks)))},
                                settings.socket_timeout_seconds)
            LOG.info("Seeder peer_id=%s listening=%s:%s file_id=%s chunks=%s",
                     args.peer_id, args.host, args.port, shared.file_id, len(shared.chunks))
            server.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Seeder stopped peer_id=%s", args.peer_id)
    except (OSError, ValueError, EOFError, ProtocolError):
        LOG.exception("Seeder failed peer_id=%s", args.peer_id)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
