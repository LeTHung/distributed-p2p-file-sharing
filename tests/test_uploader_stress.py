from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from common.framing import recv_exact, recv_json, send_json
from peer.uploader import SharedFile, UploaderServer


class UploaderStressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "binary.bin"
        path.write_bytes(bytes(range(256)) * 1024 + b"tail")
        self.shared = SharedFile(path, 16384)

    def start(self, workers=4, pending=32, timeout=1, rate=0):
        server = UploaderServer(("127.0.0.1", 0), self.shared, timeout,
                                max_workers=workers, max_pending=pending, upload_rate=rate)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def cleanup():
            server.request_stop()
            server.shutdown()
            server.server_close()
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.addCleanup(cleanup)
        return server

    def wait_for(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.01)
        self.fail("Timed out waiting for uploader state")

    def download(self, server, index, **fields):
        with socket.create_connection(server.server_address, timeout=5) as connection:
            request_id = f"stress-{index}-{threading.get_ident()}"
            send_json(connection, {"type": "get_chunk", "request_id": request_id,
                                   "file_id": self.shared.file_id, "chunk_index": index, **fields})
            header = recv_json(connection, timeout=5)
            self.assertEqual(header["request_id"], request_id)
            if header["ok"]:
                data = recv_exact(connection, header["data_length"])
                self.assertEqual(hashlib.sha256(data).hexdigest(), self.shared.chunks[index]["chunk_hash"])
                return len(data)
            return header["error_code"]

    def test_128_requests_from_24_clients_respect_capacity_and_release_all_slots(self):
        server = self.start(workers=4, pending=32)
        with ThreadPoolExecutor(max_workers=24) as pool:
            jobs = [pool.submit(self.download, server, index % len(self.shared.chunks)) for index in range(128)]
            byte_count = sum(job.result(timeout=15) for job in jobs)
        self.wait_for(lambda: server.metrics.snapshot()["finished_connections"] == 128)
        metrics = server.metrics.snapshot()
        self.assertEqual(metrics["successful_chunks"], 128)
        self.assertEqual(metrics["completed_payload_bytes"], byte_count)
        self.assertLessEqual(metrics["peak_active_connections"], 4)
        self.assertLessEqual(metrics["peak_in_flight"], 36)
        self.assertEqual(metrics["rejected_connections"], 0)
        self.assertEqual(metrics["in_flight"], 0)
        self.assertEqual(metrics["active_connections"], 0)

    def test_valid_and_invalid_requests_do_not_poison_worker_pool(self):
        server = self.start(workers=3, pending=32)
        with ThreadPoolExecutor(max_workers=16) as pool:
            jobs = [pool.submit(self.download, server, 0,
                                **({"file_id": "0" * 64} if i % 2 else {})) for i in range(40)]
            results = [job.result(timeout=10) for job in jobs]
        self.wait_for(lambda: server.metrics.snapshot()["finished_connections"] == 40)
        self.assertEqual(results.count("FILE_NOT_FOUND"), 20)
        metrics = server.metrics.snapshot()
        self.assertEqual(metrics["successful_chunks"], 20)
        self.assertEqual(metrics["error_responses"], 20)
        self.assertEqual(metrics["in_flight"], 0)
        self.assertIsInstance(self.download(server, 1), int)

    def test_incomplete_clients_timeout_then_later_download_succeeds(self):
        server = self.start(workers=3, pending=24, timeout=.2)
        sockets = []
        try:
            for _ in range(18):
                connection = socket.create_connection(server.server_address, timeout=3)
                connection.sendall(b"\x00")
                sockets.append(connection)
            self.wait_for(lambda: server.metrics.snapshot()["finished_connections"] == 18, timeout=8)
        finally:
            for connection in sockets:
                connection.close()
        self.assertEqual(server.metrics.snapshot()["connection_errors"], 18)
        self.assertIsInstance(self.download(server, 0), int)
        self.wait_for(lambda: server.metrics.snapshot()["in_flight"] == 0)

    def test_overload_rejects_new_connection_and_recovers_queued_work(self):
        server = self.start(workers=1, pending=1)
        entered, release = threading.Event(), threading.Event()
        original = self.shared.read_chunk
        def held(index):
            entered.set()
            if not release.wait(4):
                raise RuntimeError("Test did not release worker")
            return original(index)
        with patch.object(self.shared, "read_chunk", side_effect=held):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.download, server, 0)
                self.assertTrue(entered.wait(2))
                second = pool.submit(self.download, server, 1)
                try:
                    self.wait_for(lambda: server.metrics.snapshot()["accepted_connections"] == 2)
                    with socket.create_connection(server.server_address, timeout=2) as excess:
                        try:
                            self.assertEqual(excess.recv(1), b"")
                        except ConnectionResetError:
                            pass
                    self.wait_for(lambda: server.metrics.snapshot()["rejected_connections"] == 1)
                finally:
                    release.set()
                first.result(timeout=5)
                second.result(timeout=5)
        self.assertIsInstance(self.download(server, 2), int)
        self.wait_for(lambda: server.metrics.snapshot()["in_flight"] == 0)
        self.assertEqual(server.metrics.snapshot()["peak_in_flight"], 2)


if __name__ == "__main__":
    unittest.main()
