import hashlib
from pathlib import Path
import tempfile
import unittest

from common.metadata import build_metadata, validate_metadata
from tracker.errors import RequestError
from tracker.validation import validate_request


class MetadataTests(unittest.TestCase):
    def test_streamed_binary_full_file_and_short_final_chunk(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.bin"
            data = b"abc\x00def\xfftail"
            path.write_bytes(data)
            m = build_metadata(path, 4)
            self.assertEqual(m["file_id"], hashlib.sha256(data).hexdigest())
            self.assertEqual(m["file_size"], len(data))
            self.assertEqual(m["chunks"], [{"chunk_index": i // 4,
                              "chunk_hash": hashlib.sha256(data[i:i+4]).hexdigest()}
                              for i in range(0, len(data), 4)])

    def test_empty_and_exact_multiple(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "empty"
            path.touch()
            m = build_metadata(path, 4)
            self.assertEqual(m["total_chunks"], 0)
            self.assertEqual(m["chunks"], [])
            self.assertEqual(m["file_id"], hashlib.sha256(b"").hexdigest())
            path.write_bytes(b"12345678")
            self.assertEqual(build_metadata(path, 4)["total_chunks"], 2)
            for bad in (0, -1, True, 1.5):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    build_metadata(path, bad)

    def test_inconsistent_metadata_rejected(self):
        m = {"file_id": "a" * 64, "file_name": "x", "file_size": 5,
             "chunk_size": 3, "total_chunks": 2,
             "chunks": [{"chunk_index": 0, "chunk_hash": "b" * 64},
                        {"chunk_index": 1, "chunk_hash": "c" * 64}]}
        for change in ({"file_size": True}, {"total_chunks": 3}, {"file_id": "A" * 64},
                       {"chunks": [m["chunks"][0]] * 2}, {"file_name": "\0"},
                       {"chunks": [{"chunk_index": True, "chunk_hash": "b" * 64}] * 2}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_metadata({**m, **change})


class ValidationTests(unittest.TestCase):
    def test_bad_requests(self):
        requests = [{}, {"type": []}, {"type": "delete"},
                    {"type": "register_peer", "peer_id": "p", "host": "localhost", "port": 7},
                    {"type": "register_peer", "peer_id": "p", "host": "0.0.0.0", "port": 7},
                    {"type": "register_peer", "peer_id": "p", "host": "127.0.0.1", "port": True},
                    {"type": "heartbeat", "peer_id": []},
                    {"type": "get_peers", "file_id": "a" * 64, "chunk_indexes": [True]},
                    {"type": "announce_chunks", "peer_id": "p", "file_id": "a" * 64}]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(RequestError) as error:
                validate_request({"request_id": "test", **request})
            self.assertEqual(error.exception.code, "INVALID_REQUEST")
        with self.assertRaises(RequestError):
            validate_request({"type": "list_files"})

    def test_chunk_indexes_normalized_without_mutating_input(self):
        r = {"type": "get_peers", "request_id": "test", "file_id": "a" * 64,
             "chunk_indexes": [2, 0, 2]}
        self.assertEqual(validate_request(r)["chunk_indexes"], [0, 2])
        self.assertEqual(r["chunk_indexes"], [2, 0, 2])
