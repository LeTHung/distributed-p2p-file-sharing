"""Tracker registration and heartbeat for a running Seeder."""

import logging
import threading

from common.framing import ProtocolError

LOG = logging.getLogger(__name__)
RPC_ERRORS = (OSError, EOFError, ProtocolError)


class SeederRegistration:
    def __init__(self, rpc, shared_file, peer_id, host, port, interval=5):
        if interval <= 0:
            raise ValueError("heartbeat interval must be positive")
        self.rpc = rpc
        self.shared = shared_file
        self.peer_id = peer_id
        self.host, self.port = host, port
        self.interval = interval
        self.stopping = threading.Event()
        self.thread = None

    def register(self):
        self.rpc({"type": "register_peer", "peer_id": self.peer_id,
                  "host": self.host, "port": self.port})
        self.rpc({"type": "register_file", **self.shared.metadata()})
        self.rpc({"type": "announce_chunks", "peer_id": self.peer_id,
                  "file_id": self.shared.file_id,
                  "chunk_indexes": list(range(len(self.shared.chunks)))})

    def __enter__(self):
        try:
            self.register()
        except RPC_ERRORS:
            self.unregister()
            raise
        self.thread = threading.Thread(target=self._run, name="seeder-heartbeat")
        self.thread.start()
        return self

    def _run(self):
        needs_registration = False
        while not self.stopping.wait(self.interval):
            try:
                if needs_registration:
                    self.register()
                    needs_registration = False
                    LOG.info("Seeder registered again peer_id=%s", self.peer_id)
                else:
                    self.rpc({"type": "heartbeat", "peer_id": self.peer_id})
            except RPC_ERRORS:
                # A tracker restart or expired peer requires all ownership to be
                # announced again, not only register_peer. Retry on the next tick.
                needs_registration = True
                LOG.warning("Heartbeat failed; will register again peer_id=%s", self.peer_id)

    def unregister(self):
        try:
            self.rpc({"type": "unregister_peer", "peer_id": self.peer_id})
        except RPC_ERRORS:
            LOG.warning("Could not unregister peer_id=%s; Tracker TTL will remove it", self.peer_id)

    def __exit__(self, *args):
        self.stopping.set()
        if self.thread is not None:
            self.thread.join()
        self.unregister()
