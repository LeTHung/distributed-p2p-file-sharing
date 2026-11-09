"""Reusable peer-side heartbeat sender for the Tracker TCP contract."""

from __future__ import annotations

from collections.abc import Callable
import logging
import math
import socket
import threading
import uuid

from common.framing import ProtocolError, recv_json, send_json

LOG = logging.getLogger(__name__)


class TrackerHeartbeatError(RuntimeError):
    """A failed heartbeat attempt, with identifiers safe to log or act on."""

    def __init__(self, request_id: str, error_code: str):
        self.request_id = request_id
        self.error_code = error_code
        super().__init__(f"Tracker heartbeat failed ({error_code})")


FailureCallback = Callable[[TrackerHeartbeatError], None]


class PeerHeartbeat:
    """Send one framed heartbeat periodically until stopped.

    Call ``start`` after ``register_peer`` succeeds. If the peer expires,
    ``on_error`` receives ``PEER_NOT_FOUND`` so its owner can re-register and
    announce its current chunks; this helper deliberately does not guess the
    peer's address or ownership.
    """

    def __init__(
        self,
        tracker_address: tuple[str, int],
        peer_id: str,
        *,
        interval_seconds: float,
        timeout_seconds: float,
        on_error: FailureCallback | None = None,
    ):
        if (not isinstance(tracker_address, tuple) or len(tracker_address) != 2 or
                not isinstance(tracker_address[0], str) or not tracker_address[0].strip() or
                type(tracker_address[1]) is not int or not 1 <= tracker_address[1] <= 65535):
            raise ValueError("tracker_address must contain a host and valid TCP port")
        if not isinstance(peer_id, str) or not peer_id.strip():
            raise ValueError("peer_id must be a nonempty string")
        if (type(interval_seconds) not in (int, float) or not math.isfinite(interval_seconds) or
                interval_seconds <= 0):
            raise ValueError("interval_seconds must be a positive finite number")
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or
                timeout_seconds <= 0):
            raise ValueError("timeout_seconds must be a positive finite number")
        if on_error is not None and not callable(on_error):
            raise ValueError("on_error must be callable")

        self.tracker_address = tracker_address
        self.peer_id = peer_id
        self.interval_seconds = float(interval_seconds)
        self.timeout_seconds = float(timeout_seconds)
        self.on_error = on_error
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        with self._state_lock:
            return self._thread is not None and self._thread.is_alive()

    def send_once(self) -> dict:
        """Send one heartbeat and return the successful Tracker response."""
        request_id = str(uuid.uuid4())
        request = {"type": "heartbeat", "request_id": request_id, "peer_id": self.peer_id}
        try:
            with socket.create_connection(self.tracker_address, timeout=self.timeout_seconds) as connection:
                connection.settimeout(self.timeout_seconds)
                send_json(connection, request)
                response = recv_json(connection, timeout=self.timeout_seconds)
        except (OSError, EOFError, ProtocolError) as error:
            raise TrackerHeartbeatError(request_id, "TRACKER_UNAVAILABLE") from error

        if response.get("request_id") != request_id:
            raise TrackerHeartbeatError(request_id, "INVALID_RESPONSE")
        if response.get("ok") is not True:
            code = response.get("error_code")
            raise TrackerHeartbeatError(request_id, code if isinstance(code, str) and code else "INTERNAL_ERROR")
        return response

    def start(self) -> PeerHeartbeat:
        """Start the worker once; repeated calls while running are harmless."""
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return self
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name=f"tracker-heartbeat:{self.peer_id[:32]}",
                daemon=True,
            )
            self._thread.start()
        return self

    def stop(self, *, join_timeout: float | None = None) -> None:
        """Stop the worker and allow an in-flight connect plus response to time out."""
        if join_timeout is not None and (
                type(join_timeout) not in (int, float) or not math.isfinite(join_timeout) or join_timeout <= 0):
            raise ValueError("join_timeout must be a positive finite number")
        self._stop_event.set()
        with self._state_lock:
            thread = self._thread
        if thread is None or thread is threading.current_thread():
            return
        default_join_timeout = max(self.interval_seconds, 2 * self.timeout_seconds) + 1
        thread.join(join_timeout if join_timeout is not None else default_join_timeout)
        if thread.is_alive():
            raise TimeoutError("Heartbeat worker did not stop before its join timeout")
        with self._state_lock:
            if self._thread is thread:
                self._thread = None

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self.send_once()
            except TrackerHeartbeatError as error:
                LOG.warning(
                    "Tracker heartbeat failed peer_id=%s request_id=%s error_code=%s",
                    self.peer_id,
                    error.request_id,
                    error.error_code,
                )
                if self.on_error is not None:
                    try:
                        self.on_error(error)
                    except Exception as callback_error:
                        LOG.warning(
                            "Heartbeat error callback failed peer_id=%s (%s)",
                            self.peer_id,
                            type(callback_error).__name__,
                        )

    def __enter__(self) -> PeerHeartbeat:
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
