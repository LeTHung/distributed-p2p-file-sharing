"""Week-two request/response contract checks that do not require PostgreSQL."""

from copy import deepcopy
import unittest

from tracker.database import Directory
from tracker.errors import RequestError
from tracker.validation import validate_request


class Week2ValidationTests(unittest.TestCase):
    def setUp(self):
        self.announce = {"type": "announce_chunks", "request_id": "week2-request",
                         "peer_id": "seeder-a", "file_id": "a" * 64, "chunk_indexes": [0, 1]}
        self.metadata = {"type": "register_file", "request_id": "week2-register",
                         "file_id": "a" * 64, "file_name": "sample.bin", "file_size": 9,
                         "chunk_size": 4, "total_chunks": 3,
                         "chunks": [{"chunk_index": i, "chunk_hash": str(i) * 64} for i in range(3)]}

    def assert_invalid(self, request):
        with self.assertRaises(RequestError) as error:
            validate_request(request)
        self.assertEqual(error.exception.code, "INVALID_REQUEST")

    def test_announce_requires_every_mandatory_field(self):
        for field in self.announce:
            with self.subTest(field=field):
                self.assert_invalid({k: v for k, v in self.announce.items() if k != field})

    def test_announce_rejects_invalid_types_and_indexes(self):
        for indexes in (None, {}, "0,1", [-1], [True], [False], [0.0], ["0"], [None], [2**31]):
            with self.subTest(indexes=indexes):
                self.assert_invalid({**self.announce, "chunk_indexes": indexes})
        for field in ("peer_id", "file_id", "request_id"):
            for value in (None, [], 123, ""):
                with self.subTest(field=field, value=value):
                    self.assert_invalid({**self.announce, field: value})

    def test_announce_duplicate_indexes_are_additive_and_input_is_unchanged(self):
        request = {**self.announce, "chunk_indexes": [2, 0, 2, 1, 0]}
        original = deepcopy(request)
        self.assertEqual(validate_request(request)["chunk_indexes"], [0, 1, 2])
        self.assertEqual(request, original)
        self.assertEqual(validate_request({**request, "chunk_indexes": []})["chunk_indexes"], [])

    def test_tracker_index_outside_file_is_invalid_request_for_both_apis(self):
        for kind in ("announce_chunks", "get_peers"):
            for indexes in ([3], [0, 3], [2**31 - 1]):
                request = validate_request({**self.announce, "type": kind, "chunk_indexes": indexes})
                with self.subTest(kind=kind, indexes=indexes), self.assertRaises(RequestError) as error:
                    Directory._indexes(request, {"total_chunks": 3})
                self.assertEqual(error.exception.code, "INVALID_REQUEST")

    def test_register_file_rejects_local_paths_and_non_file_names(self):
        for name in (r"D:\private\sample.bin", "/home/seeder/private/sample.bin", r"..\sample.bin",
                     "../sample.bin", r"\\server\private\sample.bin", "D:sample.bin", ".", ".."):
            with self.subTest(name=name):
                self.assert_invalid({**self.metadata, "file_name": name})

    def test_register_file_matches_contract_for_empty_files_and_chunk_order(self):
        from common.metadata import EMPTY_HASH
        self.assert_invalid({**self.metadata, "file_id": EMPTY_HASH, "file_size": 0,
                             "total_chunks": 0, "chunks": []})
        self.assert_invalid({**self.metadata, "chunks": list(reversed(self.metadata["chunks"]))})
        self.assertEqual(validate_request(self.metadata), self.metadata)
