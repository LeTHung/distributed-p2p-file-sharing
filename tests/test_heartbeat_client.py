import json
import struct
import threading
import time
import unittest
from unittest.mock import patch

from common.heartbeat import PeerHeartbeat, TrackerHeartbeatError
from tracker.errors import RequestError
from tracker.server import TrackerServer


class RecordingDirectory:
    def __init__(self, *, reject=False):
        self.reject = reject
        self.requests = []
        self.lock = threading.Lock()

    def execute(self, request):
        with self.lock:
            self.requests.append(request)
        if self.reject:
            raise RequestError("PEER_NOT_FOUND", "Peer expired")
        return {"peer_id": request["peer_id"]}

    def prune_expired(self):
        return 0


class SlowFirstDirectory(RecordingDirectory):
    def __init__(self):
        super().__init__()
        self.first_request_started = threading.Event()
        self.release_first_request = threading.Event()

    def execute(self, request):
        with self.lock:
            first = not self.requests
        if first:
            self.first_request_started.set()
            self.release_first_request.wait(3)
        return super().execute(request)


class PeerHeartbeatTests(unittest.TestCase):
    def run_server(self, directory):
        server = TrackerServer(("127.0.0.1", 0), directory, cleanup_interval=10)
        thread = threading.Thread(target=server.serve_forever, name="heartbeat-test-tracker")
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_send_once_uses_contract_and_surfaces_expired_peer(self):
        directory = RecordingDirectory(reject=True)
        server = self.run_server(directory)
        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-expired", interval_seconds=1, timeout_seconds=1)

        with self.assertRaises(TrackerHeartbeatError) as raised:
            heartbeat.send_once()

        self.assertEqual(raised.exception.error_code, "PEER_NOT_FOUND")
        self.assertEqual(len(directory.requests), 1)
        request = directory.requests[0]
        self.assertEqual(request["type"], "heartbeat")
        self.assertEqual(request["peer_id"], "peer-expired")
        self.assertEqual(request["request_id"], raised.exception.request_id)

    def test_background_error_callback_receives_expired_peer(self):
        directory = RecordingDirectory(reject=True)
        server = self.run_server(directory)
        received = threading.Event()
        errors = []

        def capture(error):
            errors.append(error)
            received.set()

        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-expired",
                                  interval_seconds=0.05, timeout_seconds=1, on_error=capture)
        heartbeat.start()
        self.assertTrue(received.wait(2), "Heartbeat failure callback was not invoked")
        heartbeat.stop()

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].error_code, "PEER_NOT_FOUND")
        self.assertTrue(errors[0].request_id)

    def test_background_sender_repeats_and_stops(self):
        directory = RecordingDirectory()
        server = self.run_server(directory)
        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-live", interval_seconds=0.05, timeout_seconds=1)

        heartbeat.start()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with directory.lock:
                if len(directory.requests) >= 2:
                    break
            time.sleep(0.01)
        heartbeat.stop()

        self.assertFalse(heartbeat.is_running)
        with directory.lock:
            requests = list(directory.requests)
        self.assertGreaterEqual(len(requests), 2)
        self.assertTrue(all(request["peer_id"] == "peer-live" for request in requests))
        self.assertEqual(len({request["request_id"] for request in requests}), len(requests))

    def test_temporary_timeout_is_reported_and_sender_retries(self):
        directory = SlowFirstDirectory()
        server = self.run_server(directory)
        self.addCleanup(directory.release_first_request.set)
        received = threading.Event()
        errors = []

        def capture(error):
            errors.append(error)
            received.set()

        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-retry",
                                  interval_seconds=0.05, timeout_seconds=0.1, on_error=capture)
        heartbeat.start()
        self.assertTrue(directory.first_request_started.wait(2), "Tracker did not receive first heartbeat")
        self.assertTrue(received.wait(2), "Timed-out heartbeat was not reported")
        self.assertEqual(errors[0].error_code, "TRACKER_UNAVAILABLE")

        directory.release_first_request.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with directory.lock:
                if len(directory.requests) >= 2:
                    break
            time.sleep(0.01)
        heartbeat.stop()

        with directory.lock:
            self.assertGreaterEqual(len(directory.requests), 2)
        self.assertFalse(heartbeat.is_running)

    def test_callback_exception_does_not_stop_sender(self):
        directory = RecordingDirectory(reject=True)
        server = self.run_server(directory)
        callback_count = 0

        def broken_callback(_error):
            nonlocal callback_count
            callback_count += 1
            raise RuntimeError("manager callback failure")

        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-callback-error",
                                  interval_seconds=0.05, timeout_seconds=1,
                                  on_error=broken_callback)
        with self.assertLogs("common.heartbeat", level="WARNING") as captured:
            heartbeat.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with directory.lock:
                    if len(directory.requests) >= 2:
                        break
                time.sleep(0.01)
            heartbeat.stop()

        self.assertGreaterEqual(callback_count, 1)
        self.assertFalse(heartbeat.is_running)
        self.assertTrue(any("Heartbeat error callback failed" in line for line in captured.output))

    def test_stop_from_callback_does_not_self_join_or_leak_thread(self):
        directory = RecordingDirectory(reject=True)
        server = self.run_server(directory)
        stopped = threading.Event()
        holder = {}

        def stop_from_worker(_error):
            holder["heartbeat"].stop()
            stopped.set()

        heartbeat = PeerHeartbeat(server.server_address[:2], "peer-stop-callback",
                                  interval_seconds=0.05, timeout_seconds=1,
                                  on_error=stop_from_worker)
        holder["heartbeat"] = heartbeat
        heartbeat.start()
        self.assertTrue(stopped.wait(2), "Callback could not stop the heartbeat worker")
        heartbeat.stop()
        self.assertFalse(heartbeat.is_running)

    def test_stop_waits_for_inflight_connect_and_response_timeouts(self):
        connection_started = threading.Event()

        class DelayedConnection:
            def __init__(self):
                self.response = b""
                self.delayed_read = False

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def settimeout(self, _timeout):
                pass

            def sendall(self, frame):
                length = struct.unpack("!I", frame[:4])[0]
                request = json.loads(frame[4:4 + length].decode("utf-8"))
                response = json.dumps({"request_id": request["request_id"], "ok": True,
                                       "peer_id": request["peer_id"]}, separators=(",", ":")).encode("utf-8")
                self.response = struct.pack("!I", len(response)) + response

            def recv(self, byte_count):
                if not self.delayed_read:
                    self.delayed_read = True
                    time.sleep(1.6)
                data, self.response = self.response[:byte_count], self.response[byte_count:]
                return data

        def delayed_connect(*_args, **_kwargs):
            connection_started.set()
            time.sleep(1.6)
            return DelayedConnection()

        heartbeat = PeerHeartbeat(("127.0.0.1", 9000), "peer-slow-connect",
                                  interval_seconds=0.01, timeout_seconds=2)
        with patch("common.heartbeat.socket.create_connection", side_effect=delayed_connect):
            heartbeat.start()
            self.assertTrue(connection_started.wait(1), "Heartbeat did not start connecting")
            heartbeat.stop()

        self.assertFalse(heartbeat.is_running)

    def test_rejects_invalid_intervals_and_addresses(self):
        for interval in (0, -1, float("nan"), True):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                PeerHeartbeat(("127.0.0.1", 9000), "peer", interval_seconds=interval, timeout_seconds=1)
        for address in (("", 9000), ("127.0.0.1", 0), ("127.0.0.1", True)):
            with self.subTest(address=address), self.assertRaises(ValueError):
                PeerHeartbeat(address, "peer", interval_seconds=1, timeout_seconds=1)
