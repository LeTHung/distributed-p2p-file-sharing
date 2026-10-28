"""Concurrent TCP directory server: python -m tracker.server [--init-db-only]."""

from __future__ import annotations

import argparse
import logging
import signal
import socket
import socketserver
import threading

import psycopg

from common.config import load_settings
from common.framing import ProtocolError, recv_json, send_json
from tracker.database import Directory
from tracker.errors import RequestError
from tracker.validation import validate_request

LOG = logging.getLogger(__name__)


def error_response(request_id, code, message):
    return {"request_id": request_id, "ok": False, "error_code": code, "message": message}


class TrackerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(self.server.timeout_seconds)
        # Peers currently open one connection per RPC; also support framed RPCs in sequence.
        while not self.server.stopping.is_set():
            try:
                request = recv_json(self.request, timeout=self.server.timeout_seconds)
            except ProtocolError:
                LOG.warning("Invalid JSON frame from %s", self.client_address[0])
                self.reply(error_response(None, "INVALID_REQUEST", "Invalid JSON frame"))
                return  # Stream position cannot be trusted after a malformed frame.
            except (EOFError, OSError):
                return
            request_id = request.get("request_id")
            if not isinstance(request_id, str) or len(request_id) > 256:
                request_id = None
            try:
                result = self.server.directory.execute(validate_request(request))
                response = {**result, "request_id": request_id, "ok": True}
            except RequestError as error:
                response = error_response(request_id, error.code, str(error))
            except psycopg.Error as error:
                # Never log a DSN, password, raw SQL, or user-supplied error text.
                LOG.error("Database request failed (%s)", type(error).__name__)
                response = error_response(request_id, "INTERNAL_ERROR", "Database operation failed; retry later")
            except Exception as error:
                LOG.error("Unexpected request failure (%s)", type(error).__name__)
                response = error_response(request_id, "INTERNAL_ERROR", "Tracker operation failed")
            if not self.reply(response):
                return

    def reply(self, response):
        self.request.settimeout(self.server.timeout_seconds)
        try:
            send_json(self.request, response)
            return True
        except ProtocolError:
            try:
                send_json(self.request, error_response(response.get("request_id"), "INTERNAL_ERROR",
                                                       "Response exceeds the control message limit"))
            except OSError:
                pass
        except OSError:
            pass
        return False


class TrackerServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = False
    block_on_close = True

    def __init__(self, address, directory, *, timeout_seconds=5, max_clients=64, cleanup_interval=5):
        if timeout_seconds <= 0 or max_clients <= 0 or cleanup_interval <= 0:
            raise ValueError("Timeout, client limit and cleanup interval must be positive")
        self.directory = directory
        self.timeout_seconds = timeout_seconds
        self.cleanup_interval = cleanup_interval
        self.stopping = threading.Event()
        self._slots = threading.BoundedSemaphore(max_clients)
        self._connections = set()
        self._lock = threading.Lock()
        self._cleaner = None
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, TrackerHandler)

    def process_request(self, request, client_address):
        if self.stopping.is_set() or not self._slots.acquire(blocking=False):
            # Do not block the accept thread sending an overload response to a slow reader.
            self.shutdown_request(request)
            return
        with self._lock:
            self._connections.add(request)
        try:
            super().process_request(request, client_address)
        except Exception:
            self._release(request)
            raise

    def _release(self, request):
        with self._lock:
            self._connections.discard(request)
        self._slots.release()

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._release(request)

    def serve_forever(self, poll_interval=0.1):
        self._cleaner = threading.Thread(target=self._cleanup, name="tracker-cleanup")
        self._cleaner.start()
        try:
            super().serve_forever(poll_interval)
        finally:
            self.stopping.set()

    def _cleanup(self):
        while not self.stopping.wait(self.cleanup_interval):
            try:
                removed = self.directory.prune_expired()
                if removed:
                    LOG.info("Pruned %d expired peers", removed)
            except Exception as error:
                LOG.error("Peer cleanup failed (%s); will retry", type(error).__name__)

    def server_close(self):
        self.stopping.set()
        with self._lock:
            connections = list(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        super().server_close()
        if self._cleaner is not None:
            self._cleaner.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--init-db-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = load_settings()
        directory = Directory(settings)
        directory.initialize()
        if args.init_db_only:
            LOG.info("Tracker schema initialized")
            return
        with TrackerServer((settings.tracker_host, settings.tracker_port), directory,
                           timeout_seconds=settings.socket_timeout_seconds,
                           max_clients=settings.tracker_max_clients,
                           cleanup_interval=settings.tracker_cleanup_interval_seconds) as server:
            # shutdown() must run in a different thread from serve_forever().
            def stop(signum, frame):
                threading.Thread(target=server.shutdown, name="tracker-stop").start()

            signal.signal(signal.SIGINT, stop)
            signal.signal(signal.SIGTERM, stop)
            LOG.info("Tracker listening at %s:%d", *server.server_address[:2])
            server.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Tracker stopped")
    except (ValueError, OSError, psycopg.Error) as error:
        LOG.error("Tracker startup failed (%s); check environment and PostgreSQL", type(error).__name__)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
