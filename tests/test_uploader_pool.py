from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from common.framing import recv_exact, recv_json, send_json
from peer.uploader import SharedFile, UploaderServer


class UploaderPoolTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "binary.dat"
        self.data = bytes(range(256)) * 100 + b"tail"
        path.write_bytes(self.data)
        self.shared = SharedFile(path, 8192)

    def start_server(self, **options):
        server = UploaderServer(("127.0.0.1", 0), self.shared, 0.5, **options)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def cleanup():
            server.shutdown()
            server.server_close()
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.addCleanup(cleanup)
        return server

    def download(self, server, index):
        with socket.create_connection(server.server_address, timeout=3) as sock:
            request_id = f"client-{index}"
            send_json(sock, {"type": "get_chunk", "request_id": request_id,
                             "file_id": self.shared.file_id, "chunk_index": index})
            header = recv_json(sock)
            self.assertTrue(header["ok"], header)
            self.assertEqual(header["request_id"], request_id)
            self.assertEqual(header["chunk_index"], index)
            data = recv_exact(sock, header["data_length"])
            self.assertEqual(hashlib.sha256(data).hexdigest(),
                             self.shared.chunks[index]["chunk_hash"])
            return data

    def test_two_chunks_are_processed_at_the_same_time(self):
        server = self.start_server(max_workers=2)
        original = self.shared.read_chunk
        rendezvous = threading.Barrier(2, timeout=2)

        def concurrent_read(index):
            # A sequential server cannot pass this barrier.
            rendezvous.wait()
            return original(index)

        with patch.object(self.shared, "read_chunk", side_effect=concurrent_read):
            with ThreadPoolExecutor(max_workers=2) as clients:
                jobs = [clients.submit(self.download, server, i) for i in range(2)]
                chunks = [job.result(timeout=4) for job in jobs]
        self.assertEqual(b"".join(chunks), self.data[:16384])

    def test_many_clients_receive_correct_chunks(self):
        server = self.start_server(max_workers=3, max_pending=16)
        with ThreadPoolExecutor(max_workers=12) as clients:
            jobs = [clients.submit(self.download, server, i % len(self.shared.chunks))
                    for i in range(12)]
            for job in jobs:
                job.result(timeout=4)

    def test_capacity_is_bounded_and_slot_recovers(self):
        server = self.start_server(max_workers=1, max_pending=0)
        entered = threading.Event()
        release = threading.Event()
        original = self.shared.read_chunk

        def held_read(index):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("Test did not release worker")
            return original(index)

        with patch.object(self.shared, "read_chunk", side_effect=held_read):
            with ThreadPoolExecutor(max_workers=1) as clients:
                job = clients.submit(self.download, server, 0)
                try:
                    self.assertTrue(entered.wait(2))
                    with socket.create_connection(server.server_address, timeout=2) as excess:
                        try:
                            self.assertEqual(excess.recv(1), b"")
                        except ConnectionResetError:
                            pass  # Windows may reset a rejected connection.
                finally:
                    release.set()
                job.result(timeout=4)
        self.download(server, 1)

    def test_slow_client_does_not_block_another_worker(self):
        server = self.start_server(max_workers=2)
        with socket.create_connection(server.server_address, timeout=2) as slow:
            slow.sendall(b"\x00")  # Incomplete length header.
            self.download(server, 1)
            self.assertEqual(slow.recv(1), b"")  # Socket timeout releases the slot.
        self.download(server, 0)

    def test_unexpected_handler_error_does_not_lose_worker(self):
        server = self.start_server(max_workers=1)
        with patch.object(self.shared, "read_chunk", side_effect=RuntimeError("read failed")):
            with patch.object(server, "handle_error") as report:
                with socket.create_connection(server.server_address, timeout=2) as sock:
                    send_json(sock, {"type": "get_chunk", "request_id": "failed-request",
                                     "file_id": self.shared.file_id, "chunk_index": 0})
                    self.assertEqual(sock.recv(1), b"")
                report.assert_called_once()
        self.download(server, 0)

    def test_invalid_worker_configuration(self):
        for options in ({"max_workers": 0}, {"max_workers": True},
                        {"max_pending": -1}, {"max_pending": False}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                UploaderServer(("127.0.0.1", 0), self.shared, **options)


if __name__ == "__main__":
    unittest.main()
