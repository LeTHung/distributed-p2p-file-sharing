"""Serve only verified downloaded chunks and advertise a real listening endpoint."""

import hashlib
import logging
from pathlib import Path
import threading

from common.heartbeat import PeerHeartbeat
from common.framing import ProtocolError
from peer.uploader import UploaderServer, tracker_request

LOG = logging.getLogger(__name__)


class _OwnershipHeartbeat(PeerHeartbeat):
    def __init__(self, *args, owner, **kwargs):
        super().__init__(*args, **kwargs)
        self.owner = owner

    def send_once(self):
        result = super().send_once()
        try:
            self.owner.publish_current()
        except (OSError, EOFError, ProtocolError):
            LOG.warning("Chunk announce pending; retry next heartbeat peer_id=%s", self.peer_id)
        return result


class ChunkStore:
    def __init__(self, metadata, directory):
        self._metadata = metadata
        self.file_id = metadata["file_id"]
        self.chunks = metadata["chunks"]
        self.directory = Path(directory)

    def read_chunk(self, index):
        if type(index) is not int or not 0 <= index < len(self.chunks):
            raise IndexError("Chunk index is outside this file")
        expected_length = min(self._metadata["chunk_size"], self._metadata["file_size"] -
                              index * self._metadata["chunk_size"])
        try:
            data = (self.directory / f"{index}.part").read_bytes()
        except FileNotFoundError:
            # Atomic reassembly publishes completed/ before removing .part files.
            with (self.directory / "completed" / self._metadata["file_name"]).open("rb") as source:
                source.seek(index * self._metadata["chunk_size"])
                data = source.read(expected_length)
        if (len(data) != expected_length or
                hashlib.sha256(data).hexdigest() != self.chunks[index]["chunk_hash"]):
            raise OSError("Chunk is missing or invalid")
        return data

    def available_indexes(self):
        available = []
        for index in range(len(self.chunks)):
            try:
                self.read_chunk(index)
                available.append(index)
            except OSError:
                pass
        return available


class ChunkSharing:
    def __init__(self, tracker, metadata, directory, peer_id, *, host="127.0.0.1", port=0,
                 advertise_host=None, interval=5, timeout=5):
        name = metadata["file_name"]
        if (Path(name).name != name or "/" in name or "\\" in name or ":" in name or name in (".", "..")):
            raise ValueError("Chunk store requires a safe display filename")
        if host in ("0.0.0.0", "::") and not advertise_host:
            raise ValueError("Wildcard bind requires advertise_host")
        self.tracker, self.peer_id = tracker, peer_id
        self.metadata = metadata
        self.store = ChunkStore(metadata, directory)
        self.host, self.port = host, port
        self.advertise_host = advertise_host or host
        self.interval, self.timeout = interval, timeout
        self.closing = threading.Event()
        self.server = self.thread = self.heartbeat = None
        self._available = set()
        self._available_lock = threading.Lock()

    def rpc(self, payload):
        return tracker_request(self.tracker, payload, self.timeout)

    def register(self):
        self.rpc({"type": "register_peer", "peer_id": self.peer_id,
                  "host": self.advertise_host, "port": self.server.server_address[1]})
        self.rpc({"type": "register_file", **self.metadata})
        with self._available_lock:
            self._available.update(self.store.available_indexes())
        self.publish_current()

    def add_chunk(self, index):
        with self._available_lock:
            self._available.add(index)

    def publish_current(self):
        with self._available_lock:
            indexes = sorted(self._available)
        self.rpc({"type": "announce_chunks", "peer_id": self.peer_id,
                  "file_id": self.store.file_id, "chunk_indexes": indexes})

    def recover(self, error):
        if not self.closing.is_set():
            try:
                self.register()
            except (OSError, EOFError, ProtocolError):
                LOG.warning("Sharing registration recovery failed peer_id=%s", self.peer_id)

    def __enter__(self):
        self.server = UploaderServer((self.host, self.port), self.store, self.timeout, max_workers=2)
        self.thread = threading.Thread(target=self.server.serve_forever, name="leecher-chunk-server")
        self.thread.start()
        try:
            self.register()
            self.heartbeat = _OwnershipHeartbeat(self.tracker, self.peer_id, interval_seconds=self.interval,
                                                 timeout_seconds=self.timeout, on_error=self.recover, owner=self)
            self.heartbeat.start()
            LOG.info("Sharing verified chunks peer_id=%s host=%s port=%s", self.peer_id,
                     self.advertise_host, self.server.server_address[1])
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *args):
        self.closing.set()
        try:
            if self.heartbeat:
                self.heartbeat.stop(join_timeout=5 * self.timeout + 2)
        finally:
            if self.server:
                self.server.request_stop()
                self.server.shutdown()
                self.server.server_close()
                self.thread.join()
            try:
                self.rpc({"type": "unregister_peer", "peer_id": self.peer_id})
            except (OSError, EOFError, ProtocolError):
                LOG.warning("Sharing unregister failed; Tracker TTL will expire peer_id=%s", self.peer_id)
