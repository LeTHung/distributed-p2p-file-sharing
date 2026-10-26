import hashlib
from pathlib import Path
import socket
import tempfile
import threading
import unittest

from common.framing import recv_exact, recv_json, send_json
from peer.uploader import SharedFile, UploaderServer, tracker_request


class UploaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "sample.bin"
        self.data = b"hello\x00world\xfftail"
        self.path.write_bytes(self.data)
        self.shared = SharedFile(self.path, 6)
        self.server = UploaderServer(("127.0.0.1", 0), self.shared, 1)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def request(self, **fields):
        with socket.create_connection(self.server.server_address, timeout=2) as sock:
            send_json(sock, {"type": "get_chunk", "request_id": "test-request",
                             "file_id": self.shared.file_id, "chunk_index": 0, **fields})
            header = recv_json(sock)
            data = recv_exact(sock, header["data_length"]) if header["ok"] else sock.recv(1)
            return header, data

    def test_binary_chunks_reconstruct_original_and_match_hashes(self):
        result = bytearray()
        for chunk in self.shared.chunks:
            header, data = self.request(chunk_index=chunk["chunk_index"])
            self.assertTrue(header["ok"])
            self.assertEqual(header["request_id"], "test-request")
            self.assertEqual(hashlib.sha256(data).hexdigest(), chunk["chunk_hash"])
            result.extend(data)
        self.assertEqual(result, self.data)
        self.assertEqual(hashlib.sha256(result).hexdigest(), self.shared.file_id)
        self.assertEqual(header["data_length"], len(self.data) % 6)

    def test_errors_have_no_binary_body_and_server_remains_available(self):
        for fields, code in [({"file_id": "unknown"}, "FILE_NOT_FOUND"),
                             ({"chunk_index": -1}, "CHUNK_NOT_FOUND"),
                             ({"chunk_index": 99}, "CHUNK_NOT_FOUND"),
                             ({"chunk_index": True}, "INVALID_REQUEST"),
                             ({"type": "delete"}, "INVALID_REQUEST")]:
            with self.subTest(fields=fields):
                header, data = self.request(**fields)
                self.assertFalse(header["ok"])
                self.assertEqual(header["error_code"], code)
                self.assertEqual(data, b"")
        self.assertTrue(self.request()[0]["ok"])

    def test_changed_source_is_not_uploaded(self):
        self.path.write_bytes(b"changed")
        self.assertEqual(self.request()[0]["error_code"], "CHUNK_UNAVAILABLE")

    def test_empty_file_and_invalid_chunk_size(self):
        self.path.write_bytes(b"")
        with self.assertRaises(ValueError):
            SharedFile(self.path)
        with self.assertRaises(ValueError):
            SharedFile(self.path, 0)

    def test_tracker_rpc_sends_framed_request_and_checks_reply(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(2)
            captured = []

            def tracker():
                with listener.accept()[0] as sock:
                    sock.settimeout(2)
                    request = recv_json(sock)
                    captured.append(request)
                    send_json(sock, {"request_id": request["request_id"], "ok": True})

            thread = threading.Thread(target=tracker, daemon=True)
            thread.start()
            response = tracker_request(listener.getsockname(),
                                       {"type": "register_file", **self.shared.metadata()}, 2)
            thread.join(2)
            self.assertTrue(response["ok"])
            self.assertEqual(captured[0]["chunks"], self.shared.chunks)
            self.assertEqual(captured[0]["file_id"], self.shared.file_id)


if __name__ == "__main__":
    unittest.main()
