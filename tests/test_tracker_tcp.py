from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import socket
import struct
import threading
import time
import unittest
import uuid

import psycopg

from common.framing import MAX_JSON_MESSAGE_BYTES, ProtocolError, recv_exact, recv_json, send_json
from tracker.server import TrackerServer


@contextmanager
def running_server(directory, **kwargs):
    server = TrackerServer(("127.0.0.1", 0), directory, **kwargs)
    thread = threading.Thread(target=server.serve_forever, name="test-tracker")
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)
        if thread.is_alive():
            raise AssertionError("Tracker thread did not stop")


def rpc(address, payload):
    request = {"request_id": str(uuid.uuid4()), **payload}
    with socket.create_connection(address, timeout=5) as sock:
        send_json(sock, request)
        response = recv_json(sock)
    if response["request_id"] != request["request_id"]:
        raise AssertionError("Response request ID does not match")
    return response


class TransportDirectory:
    def execute(self, request):
        if request.get("fail_db"):
            raise psycopg.OperationalError("private connection details must not reach client")
        if request.get("large"):
            return {"files": ["x" * MAX_JSON_MESSAGE_BYTES]}
        return {"files": []}

    def prune_expired(self):
        return 0


class TrackerTCPTests(unittest.TestCase):
    def setUp(self):
        self.context = running_server(TransportDirectory(), timeout_seconds=0.5, cleanup_interval=0.05)
        self.server = self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)
        self.address = self.server.server_address

    def test_fragmented_and_multiple_frames_on_same_connection(self):
        with socket.create_connection(self.address, timeout=2) as sock:
            r = {"type": "list_files", "request_id": "fragment"}
            data = json.dumps(r).encode()
            frame = struct.pack("!I", len(data)) + data
            for byte in frame:
                sock.sendall(bytes([byte]))
            self.assertEqual(recv_json(sock), {"ok": True, "request_id": "fragment", "files": []})
            send_json(sock, {**r, "request_id": "second"})
            self.assertEqual(recv_json(sock)["request_id"], "second")

    def test_invalid_frames_and_disconnects_do_not_stop_server(self):
        for payload in (b"{", b"[]", b"\xff", b'{"x":NaN}', b"[" * 2000):
            with self.subTest(payload=payload[:20]), socket.create_connection(self.address, timeout=2) as sock:
                sock.sendall(struct.pack("!I", len(payload)) + payload)
                self.assertEqual(recv_json(sock)["error_code"], "INVALID_REQUEST")
                self.assertEqual(sock.recv(1), b"")
        for length in (0, MAX_JSON_MESSAGE_BYTES + 1, 2**32-1):
            with socket.create_connection(self.address, timeout=2) as sock:
                sock.sendall(struct.pack("!I", length))
                self.assertEqual(recv_json(sock)["error_code"], "INVALID_REQUEST")
        for fragment in (b"\0\0", struct.pack("!I", 10) + b"{"):
            with socket.create_connection(self.address, timeout=2) as sock:
                sock.sendall(fragment)
                sock.shutdown(socket.SHUT_WR)
                self.assertEqual(sock.recv(1), b"")
        self.assertTrue(rpc(self.address, {"type": "list_files"})["ok"])

    def test_validation_and_db_error_then_recovery(self):
        for payload, code in (({"type": "bogus"}, "INVALID_REQUEST"),
                              ({"type": "heartbeat"}, "INVALID_REQUEST"),
                              ({"type": "list_files", "fail_db": True}, "INTERNAL_ERROR"),
                              ({"type": "list_files", "large": True}, "INTERNAL_ERROR")):
            response = rpc(self.address, payload)
            self.assertFalse(response["ok"])
            self.assertEqual(response["error_code"], code)
            self.assertNotIn("private", json.dumps(response))
        self.assertTrue(rpc(self.address, {"type": "list_files"})["ok"])

    def test_concurrent_clients_and_idle_timeout(self):
        with socket.create_connection(self.address, timeout=2) as idle:
            with ThreadPoolExecutor(max_workers=16) as pool:
                replies = list(pool.map(lambda _: rpc(self.address, {"type": "list_files"}), range(80)))
            self.assertTrue(all(r["ok"] for r in replies))
            self.assertEqual(idle.recv(1), b"")

    def test_shutdown_closes_incomplete_clients(self):
        client = socket.create_connection(self.address, timeout=2)
        self.addCleanup(client.close)
        client.sendall(b"\0")
        # The common cleanup stops the listener and joins all worker threads.

    def test_client_limit_does_not_block_accept_loop_and_slot_is_released(self):
        with running_server(TransportDirectory(), max_clients=1, timeout_seconds=0.15) as server:
            with socket.create_connection(server.server_address, timeout=2) as idle:
                # A completed response proves this connection owns the sole worker.
                send_json(idle, {"type": "list_files", "request_id": "idle"})
                self.assertTrue(recv_json(idle)["ok"])
                with socket.create_connection(server.server_address, timeout=2) as rejected:
                    try:
                        self.assertEqual(rejected.recv(1), b"")
                    except ConnectionResetError:
                        pass
                self.assertEqual(idle.recv(1), b"")
            self.assertTrue(rpc(server.server_address, {"type": "list_files"})["ok"])


class FramingEdgeTests(unittest.TestCase):
    def test_unsigned_big_endian_and_unicode_byte_length(self):
        a, b = socket.socketpair()
        with a, b:
            a.settimeout(2)
            b.settimeout(2)
            send_json(a, {"v": "Việt"})
            size = struct.unpack("!I", recv_exact(b, 4))[0]
            data = recv_exact(b, size)
            self.assertEqual(size, len(data))
            self.assertEqual(json.loads(data), {"v": "Việt"})

    def test_send_rejects_non_json_and_oversized(self):
        a, b = socket.socketpair()
        with a, b:
            for value in ([], {"n": float("nan")}, {"x": "a" * MAX_JSON_MESSAGE_BYTES}):
                with self.subTest(value_type=type(value)), self.assertRaises(ProtocolError):
                    send_json(a, value)

    def test_absolute_deadline_for_incomplete_header(self):
        a, b = socket.socketpair()
        with a, b:
            a.sendall(b"\0")
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                recv_json(b, timeout=0.1)
            self.assertLess(time.monotonic() - start, 1)
