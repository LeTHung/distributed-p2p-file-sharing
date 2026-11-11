from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest

from common.framing import recv_exact, recv_json, send_json
from peer.uploader import SharedFile, UploaderServer
from peer.upload_policy import UploadPolicy


class UploadPolicyTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "sample.bin"
        path.write_bytes(bytes(range(256)) * 32 + b"tail")
        self.shared = SharedFile(path, 4096)

    def server(self, **options):
        server = UploaderServer(("127.0.0.1", 0), self.shared, timeout_seconds=1,
                                max_workers=2, **options)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close():
            server.request_stop()
            server.shutdown()
            server.server_close()
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.addCleanup(close)
        return server

    def connect(self, server, index):
        sock = socket.create_connection(server.server_address, timeout=5)
        try:
            send_json(sock, {"type": "get_chunk", "request_id": f"rate-test-{index}",
                             "file_id": self.shared.file_id, "chunk_index": index})
            header = recv_json(sock)
            self.assertTrue(header["ok"], header)
            return sock, header
        except BaseException:
            sock.close()
            raise

    def download(self, server, index):
        sock, header = self.connect(server, index)
        with sock:
            data = recv_exact(sock, header["data_length"])
        self.assertEqual(data, self.shared.read_chunk(index))
        return data

    def test_rate_is_aggregate_across_two_clients(self):
        server = self.server(upload_rate=8192)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as clients:
            jobs = [clients.submit(self.download, server, index) for index in (0, 1)]
            self.assertEqual(sum(len(job.result(timeout=10)) for job in jobs), 8192)
        # Per-worker throttles would finish in about 0.5s. Aggregate must take >=1s.
        self.assertGreaterEqual(time.monotonic() - started, 0.95)

    def test_short_final_chunk_transfers_with_rate_disabled(self):
        server = self.server(upload_rate=0)
        self.assertEqual(self.download(server, 2), b"tail")

    def test_stop_interrupts_extremely_slow_upload_and_queued_clients(self):
        server = self.server(upload_rate=1, max_pending=2)
        sock, header = self.connect(server, 0)
        self.addCleanup(sock.close)
        idle = socket.create_connection(server.server_address, timeout=2)
        queued = socket.create_connection(server.server_address, timeout=2)
        self.addCleanup(idle.close)
        self.addCleanup(queued.close)
        started = time.monotonic()
        server.request_stop()
        server.shutdown()
        server.server_close()
        self.assertLess(time.monotonic() - started, 2)
        try:
            self.assertEqual(sock.recv(1), b"")
        except ConnectionResetError:
            pass

    def test_restart_on_same_port_and_file_serves_identical_chunk(self):
        first = self.server(upload_rate=0)
        address = first.server_address
        expected = self.download(first, 0)
        first.shutdown()
        first.server_close()
        with UploaderServer(address, self.shared) as second:
            worker = threading.Thread(target=second.serve_forever, daemon=True)
            worker.start()
            try:
                self.assertEqual(self.download(second, 0), expected)
            finally:
                second.shutdown()
                worker.join(2)

    def test_invalid_rates_do_not_bind_a_socket(self):
        for rate in (-1, True, 1.5):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                UploadPolicy(rate)
