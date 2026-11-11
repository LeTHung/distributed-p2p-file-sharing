"""Real PostgreSQL tests, opt in with TRACKER_TEST_DSN.

Each test creates a unique schema and drops only that owned schema afterward.
No existing tables or schemas are truncated or removed.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from common.config import load_settings
from common.metadata import build_metadata
from tracker.database import Directory
from tracker.errors import RequestError
from tracker.validation import validate_request
from test_tracker_tcp import rpc, running_server


@unittest.skipUnless(os.getenv("TRACKER_TEST_DSN"), "Set TRACKER_TEST_DSN for a real, disposable PostgreSQL test schema")
class PostgreSQLTests(unittest.TestCase):
    def setUp(self):
        self.schema = "tracker_test_" + uuid.uuid4().hex
        self.dsn = os.environ["TRACKER_TEST_DSN"]
        with psycopg.connect(self.dsn, connect_timeout=5) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.addCleanup(self.drop_schema)
        settings = replace(load_settings(), peer_ttl_seconds=20)
        self.directory = Directory(settings, self.connect)
        self.directory.initialize()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.bin"
            path.write_bytes(b"abcdefghij")
            self.metadata = build_metadata(path, 4)

    def connect(self):
        return psycopg.connect(self.dsn, connect_timeout=5, row_factory=dict_row,
                              options=f"-c search_path={self.schema} -c statement_timeout=5000 -c lock_timeout=3000")

    def drop_schema(self):
        with psycopg.connect(self.dsn, connect_timeout=5) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def request(self, kind, **fields):
        return self.directory.execute(validate_request({"type": kind, "request_id": str(uuid.uuid4()), **fields}))

    def peer(self, name, indexes=None):
        self.request("register_peer", peer_id=name, host="127.0.0.1", port=7001)
        if indexes is not None:
            self.request("announce_chunks", peer_id=name, file_id=self.metadata["file_id"], chunk_indexes=indexes)

    def expire(self, peer_id):
        with self.connect() as conn:
            conn.execute("UPDATE peers SET last_seen=clock_timestamp()-INTERVAL '1 hour' WHERE peer_id=%s", (peer_id,))

    def test_schema_restart_and_upgrade_preserve_registered_data(self):
        self.request("register_file", **self.metadata)
        with self.connect() as conn:
            conn.execute("ALTER TABLE files DROP CONSTRAINT files_total_chunks_check")
            conn.execute("ALTER TABLE files ADD CONSTRAINT files_total_chunks_check CHECK (total_chunks>0)")
        self.directory.initialize()
        self.directory.initialize()
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM files").fetchone()["n"], 1)
        empty = {"file_id": hashlib.sha256(b"").hexdigest(), "file_name": "empty", "file_size": 0,
                 "chunk_size": 4, "total_chunks": 0, "chunks": []}
        # The old schema migration remains safe, but new wire registrations must
        # follow the current group contract, which excludes empty files.
        with self.assertRaises(RequestError) as error:
            self.request("register_file", **empty)
        self.assertEqual(error.exception.code, "INVALID_REQUEST")

    def test_directory_filters_ownership_expiry_self_and_duplicates(self):
        self.request("register_file", **self.metadata)
        for name, indexes in (("a", [0, 1]), ("b", [1, 2]), ("wrong", [2]),
                              ("expired", [0, 1]), ("unannounced", None), ("inactive", [1])):
            self.peer(name, indexes)
        self.expire("expired")
        with self.connect() as conn:
            conn.execute("UPDATE peers SET status='inactive' WHERE peer_id='inactive'")
        r = self.request("get_peers", file_id=self.metadata["file_id"], chunk_indexes=[1, 1])
        self.assertEqual([p["peer_id"] for p in r["peers"]], ["a", "b"])
        self.assertTrue(all(p["chunk_indexes"] == [1] for p in r["peers"]))
        self.assertEqual(r["file"]["chunks"], self.metadata["chunks"])
        r = self.request("get_peers", file_id=self.metadata["file_id"], peer_id="a", chunk_indexes=[1])
        self.assertEqual([p["peer_id"] for p in r["peers"]], ["b"])
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"], chunk_indexes=[])["peers"], [])

    def test_idempotency_conflicts_and_transaction_rollback(self):
        self.request("register_file", **self.metadata)
        self.peer("p", [0, 0])
        self.request("register_file", **self.metadata)
        self.peer("p", [0, 1])
        changed = {**self.metadata, "chunks": [{**c, "chunk_hash": "e" * 64} for c in self.metadata["chunks"]]}
        for metadata in (changed, {**self.metadata, "chunk_size": 5, "total_chunks": 2,
                                  "chunks": self.metadata["chunks"][:2]}):
            with self.assertRaises(RequestError) as error:
                self.request("register_file", **metadata)
            self.assertEqual(error.exception.code, "INVALID_REQUEST")
        with self.assertRaises(RequestError):
            self.request("announce_chunks", peer_id="p", file_id=self.metadata["file_id"], chunk_indexes=[2, 99])
        r = self.request("get_peers", file_id=self.metadata["file_id"])
        self.assertEqual(r["file"]["chunks"], self.metadata["chunks"])
        self.assertEqual(r["peers"][0]["chunk_indexes"], [0, 1])
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peers").fetchone()["n"], 1)
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM files").fetchone()["n"], 1)

    def test_heartbeat_prune_unregister_and_list_files(self):
        self.request("register_file", **self.metadata)
        self.assertEqual(self.request("list_files")["files"], [])
        self.peer("alive", [0])
        self.peer("expired", [0])
        self.expire("expired")
        self.request("heartbeat", peer_id="alive")
        with self.assertRaises(RequestError):
            self.request("heartbeat", peer_id="expired")
        self.assertEqual(self.directory.prune_expired(), 1)
        self.assertEqual(len(self.request("list_files")["files"]), 1)
        self.request("unregister_peer", peer_id="alive")
        self.assertEqual(self.request("list_files")["files"], [])
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"], [])
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 0)
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM chunks").fetchone()["n"], 3)

    def test_not_found_errors_and_foreign_keys(self):
        for kind, fields, code in (("get_peers", {"file_id": "a" * 64}, "FILE_NOT_FOUND"),
                                  ("heartbeat", {"peer_id": "missing"}, "PEER_NOT_FOUND"),
                                  ("unregister_peer", {"peer_id": "missing"}, "PEER_NOT_FOUND")):
            with self.assertRaises(RequestError) as error:
                self.request(kind, **fields)
            self.assertEqual(error.exception.code, code)
        self.request("register_file", **self.metadata)
        with self.assertRaises(RequestError) as error:
            self.request("get_peers", file_id=self.metadata["file_id"], chunk_indexes=[99])
        self.assertEqual(error.exception.code, "INVALID_REQUEST")
        with self.assertRaises(psycopg.errors.ForeignKeyViolation), self.connect() as conn:
            conn.execute("INSERT INTO peer_chunks (peer_id,file_id,chunk_index) VALUES (%s,%s,0)",
                         ("missing", self.metadata["file_id"]))
        # A failed transaction does not poison the connection used by the next request.
        self.peer("later", [0])

    def test_real_tcp_concurrent_registration_heartbeat_and_queries(self):
        with running_server(self.directory, timeout_seconds=5, max_clients=32, cleanup_interval=0.1) as server:
            def client(i):
                address = server.server_address
                name = f"peer-{i}"
                for payload in ({"type": "register_peer", "peer_id": name, "host": "127.0.0.1", "port": 7001+i},
                                {"type": "register_file", **self.metadata},
                                {"type": "announce_chunks", "peer_id": name, "file_id": self.metadata["file_id"],
                                 "chunk_indexes": [i % 3, i % 3]},
                                {"type": "heartbeat", "peer_id": name},
                                {"type": "get_peers", "file_id": self.metadata["file_id"]}):
                    response = rpc(address, payload)
                    self.assertTrue(response["ok"], response)
            with ThreadPoolExecutor(max_workers=12) as pool:
                list(pool.map(client, range(24)))
            peers = rpc(server.server_address, {"type": "get_peers", "file_id": self.metadata["file_id"]})["peers"]
            self.assertEqual(len(peers), 24)
            self.assertEqual(len({p["peer_id"] for p in peers}), 24)
            self.assertTrue(all(len(p["chunk_indexes"]) == 1 for p in peers))

    def test_prune_does_not_delete_peer_with_uncommitted_heartbeat(self):
        self.peer("racing")
        self.expire("racing")
        # Hold a refreshed row uncommitted so a DELETE using the old visible
        # timestamp must wait, then recheck against the committed heartbeat.
        conn = self.connect()
        self.addCleanup(conn.close)
        conn.execute("UPDATE peers SET last_seen=clock_timestamp() WHERE peer_id='racing'")
        started = threading.Event()
        def prune():
            started.set()
            return self.directory.prune_expired()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(prune)
            self.assertTrue(started.wait(2))
            deadline = time.monotonic() + 2
            blocked = False
            # Observe the actual row-lock wait before allowing the heartbeat to commit.
            with self.connect() as observer:
                while time.monotonic() < deadline:
                    observer.execute("SELECT pg_stat_clear_snapshot()")
                    blocked = observer.execute("""SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                            WHERE pid<>pg_backend_pid() AND wait_event_type='Lock'
                            AND query LIKE 'DELETE FROM peers WHERE last_seen%%') AS waiting""").fetchone()["waiting"]
                    if blocked:
                        break
                    threading.Event().wait(0.01)
            # Always release the lock, including if the assertion below would fail.
            conn.commit()
            self.assertTrue(blocked, "Pruner did not reach the expected row-lock wait")
            self.assertEqual(future.result(5), 0)
        self.request("heartbeat", peer_id="racing")

    def test_background_cleanup_cascades(self):
        self.request("register_file", **self.metadata)
        self.peer("expired", [0])
        self.expire("expired")
        done = threading.Event()
        original = self.directory.prune_expired
        def prune():
            count = original()
            if count:
                done.set()
            return count
        self.directory.prune_expired = prune
        with running_server(self.directory, cleanup_interval=0.05):
            self.assertTrue(done.wait(3))
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 0)

    def test_rejoining_expired_peer_must_reannounce_ownership(self):
        self.request("register_file", **self.metadata)
        self.peer("returning", [0])
        self.expire("returning")
        self.peer("returning")
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"], [])
        self.peer("returning", [1])
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"][0]["chunk_indexes"], [1])
        self.request("register_peer", peer_id="returning", host="127.0.0.1", port=7002)
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"], [])

    def test_concurrent_duplicate_announce_and_unregister_are_atomic(self):
        self.request("register_file", **self.metadata)
        self.peer("shared")
        def announce(i):
            self.request("announce_chunks", peer_id="shared", file_id=self.metadata["file_id"], chunk_indexes=[i % 3])
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(announce, range(24)))
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"][0]["chunk_indexes"], [0, 1, 2])
        gate = threading.Barrier(3)
        def racing_announce():
            gate.wait(timeout=3)
            try:
                announce(0)
                return "OK"
            except RequestError as error:
                return error.code
        def unregister():
            gate.wait(timeout=3)
            self.request("unregister_peer", peer_id="shared")
        with ThreadPoolExecutor(max_workers=2) as pool:
            update = pool.submit(racing_announce)
            removal = pool.submit(unregister)
            gate.wait(timeout=3)
            self.assertIn(update.result(5), ("OK", "PEER_NOT_FOUND"))
            removal.result(5)
        self.assertEqual(self.request("get_peers", file_id=self.metadata["file_id"])["peers"], [])
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 0)

    def test_real_database_error_over_tcp_then_recovery(self):
        original = self.directory._connect
        def wrong_schema():
            return psycopg.connect(self.dsn, connect_timeout=5,
                                   options="-c search_path=tracker_schema_that_does_not_exist", row_factory=dict_row)
        self.directory._connect = wrong_schema
        with running_server(self.directory, cleanup_interval=10) as server:
            response = rpc(server.server_address, {"type": "list_files"})
            self.assertEqual(response["error_code"], "INTERNAL_ERROR")
            self.directory._connect = original
            self.assertTrue(rpc(server.server_address, {"type": "list_files"})["ok"])

    def test_week2_announce_is_idempotent_additive_and_deduplicated_over_tcp(self):
        self.request("register_file", **self.metadata)
        self.peer("source")
        payload = {"type": "announce_chunks", "request_id": str(uuid.uuid4()), "peer_id": "source",
                   "file_id": self.metadata["file_id"], "chunk_indexes": [2, 0, 0, 2]}
        with running_server(self.directory) as server:
            first = rpc(server.server_address, payload)
            self.assertTrue(first["ok"])
            with self.connect() as conn:
                before = conn.execute("SELECT chunk_index,announced_at FROM peer_chunks ORDER BY chunk_index").fetchall()
            for _ in range(3):
                self.assertEqual(rpc(server.server_address, payload), first)
            with self.connect() as conn:
                after = conn.execute("SELECT chunk_index,announced_at FROM peer_chunks ORDER BY chunk_index").fetchall()
            self.assertEqual(before, after)
            self.assertEqual([r["chunk_index"] for r in after], [0, 2])
            self.assertTrue(rpc(server.server_address, {**payload, "chunk_indexes": []})["ok"])
            self.assertTrue(rpc(server.server_address, {**payload, "chunk_indexes": [1]})["ok"])
        with self.connect() as conn:
            rows = conn.execute("SELECT chunk_index FROM peer_chunks ORDER BY chunk_index").fetchall()
        self.assertEqual([r["chunk_index"] for r in rows], [0, 1, 2])

    def test_week2_invalid_announces_do_not_write_partial_ownership(self):
        self.request("register_file", **self.metadata)
        self.peer("source", [2])
        self.peer("offline", [1])
        with self.connect() as conn:
            conn.execute("UPDATE peers SET status='inactive' WHERE peer_id='offline'")
            original = conn.execute("SELECT * FROM peer_chunks ORDER BY peer_id,chunk_index").fetchall()
        base = {"type": "announce_chunks", "peer_id": "source", "file_id": self.metadata["file_id"],
                "chunk_indexes": [0]}
        cases = [({**base, "chunk_indexes": indexes}, "INVALID_REQUEST")
                 for indexes in ([-1], [True], [0, 3], [99], [0.5], ["0"], None, "0", {})]
        cases += [({k: v for k, v in base.items() if k != field}, "INVALID_REQUEST")
                  for field in ("peer_id", "file_id", "chunk_indexes")]
        cases += [({**base, "peer_id": "missing"}, "PEER_NOT_FOUND"),
                  ({**base, "peer_id": "offline"}, "PEER_NOT_FOUND"),
                  ({**base, "file_id": "f" * 64}, "FILE_NOT_FOUND")]
        with running_server(self.directory) as server:
            for request, code in cases:
                with self.subTest(request=request):
                    response = rpc(server.server_address, request)
                    self.assertFalse(response["ok"])
                    self.assertEqual(response["error_code"], code)
                    self.assertEqual(set(response), {"ok", "request_id", "error_code", "message"})
                    with self.connect() as conn:
                        rows = conn.execute("SELECT * FROM peer_chunks ORDER BY peer_id,chunk_index").fetchall()
                    self.assertEqual(rows, original)

    def test_week2_sql_failure_rolls_back_an_entire_announce_batch(self):
        self.request("register_file", **self.metadata)
        self.peer("source")
        with self.connect() as conn:
            # Test-only failure on the second index proves the first INSERT is
            # rolled back too, rather than just testing validation before writes.
            conn.execute("""CREATE FUNCTION reject_second_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
                         BEGIN
                           IF NEW.chunk_index=1 THEN
                             RAISE EXCEPTION 'private-week2-db-error';
                           END IF;
                           RETURN NEW;
                         END $$""")
            conn.execute("""CREATE TRIGGER reject_second_chunk BEFORE INSERT ON peer_chunks
                         FOR EACH ROW EXECUTE FUNCTION reject_second_chunk()""")
        payload = {"type": "announce_chunks", "peer_id": "source", "file_id": self.metadata["file_id"],
                   "chunk_indexes": [0, 1]}
        with running_server(self.directory) as server:
            response = rpc(server.server_address, payload)
            self.assertEqual(response["error_code"], "INTERNAL_ERROR")
            self.assertNotIn("private-week2-db-error", json.dumps(response))
            self.assertNotIn("Traceback", json.dumps(response))
            with self.connect() as conn:
                self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 0)
                conn.execute("DROP TRIGGER reject_second_chunk ON peer_chunks")
            self.assertTrue(rpc(server.server_address, payload)["ok"])
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 2)

    def test_week2_list_files_lists_only_unique_files_with_active_owners(self):
        self.assertEqual(self.request("list_files"), {"files": []})
        self.request("register_file", **self.metadata)
        self.assertEqual(self.request("list_files"), {"files": []})
        with tempfile.TemporaryDirectory() as folder:
            for name, data in (("unannounced.bin", b"unannounced data"), ("offline.bin", b"offline data")):
                path = Path(folder) / name
                path.write_bytes(data)
                metadata = build_metadata(path, 4)
                self.request("register_file", **metadata)
                if name == "offline.bin":
                    self.peer("offline")
                    self.request("announce_chunks", peer_id="offline", file_id=metadata["file_id"], chunk_indexes=[0])
        with self.connect() as conn:
            conn.execute("UPDATE peers SET status='inactive' WHERE peer_id='offline'")
        self.peer("first", [0, 1, 2])
        self.peer("second", [0, 1, 2])
        expected = {k: v for k, v in self.metadata.items() if k != "chunks"}
        with running_server(self.directory) as server:
            response = rpc(server.server_address, {"type": "list_files"})
            self.assertTrue(response["ok"])
            self.assertEqual(set(response), {"request_id", "ok", "files"})
            self.assertEqual(response["files"], [expected])
        self.request("unregister_peer", peer_id="first")
        self.request("unregister_peer", peer_id="second")
        self.assertEqual(self.request("list_files"), {"files": []})

    def test_week2_get_peers_returns_nested_metadata_and_requested_intersections(self):
        self.request("register_file", **self.metadata)
        self.peer("a", [0, 1])
        self.peer("b", [1, 2])
        self.peer("inactive", [0, 1, 2])
        with self.connect() as conn:
            conn.execute("UPDATE peers SET status='inactive' WHERE peer_id='inactive'")
        base = {"type": "get_peers", "file_id": self.metadata["file_id"]}
        with running_server(self.directory) as server:
            response = rpc(server.server_address, {**base, "chunk_indexes": [2, 1, 1]})
            self.assertTrue({"request_id", "ok", "file_id", "file", "peers"}.issubset(response))
            self.assertTrue(response["ok"])
            self.assertEqual(response["file_id"], self.metadata["file_id"])
            self.assertEqual(response["file"], self.metadata)
            # `develop` keeps legacy top-level metadata fields for existing clients.
            self.assertEqual({key: response[key] for key in self.metadata}, self.metadata)
            self.assertEqual([(p["peer_id"], p["chunk_indexes"]) for p in response["peers"]],
                             [("a", [1]), ("b", [1, 2])])
            all_chunks = rpc(server.server_address, base)
            self.assertEqual([p["chunk_indexes"] for p in all_chunks["peers"]], [[0, 1], [1, 2]])
            empty = rpc(server.server_address, {**base, "chunk_indexes": []})
            self.assertEqual(empty["file"], self.metadata)
            self.assertEqual(empty["peers"], [])
            self.assertEqual(rpc(server.server_address, {**base, "chunk_indexes": [0, 3]})["error_code"], "INVALID_REQUEST")
            with self.connect() as conn:
                conn.execute("UPDATE peers SET status='inactive'")
            no_sources = rpc(server.server_address, base)
            self.assertTrue(no_sources["ok"])
            self.assertEqual(no_sources["file"], self.metadata)
            self.assertEqual(no_sources["peers"], [])

    def test_week2_directory_never_returns_legacy_seeder_paths(self):
        self.request("register_file", **self.metadata)
        self.peer("source", [0])
        with running_server(self.directory) as server:
            for name in (r"D:\private-week2\sample.bin", "/home/private-week2/sample.bin",
                         r"\\private-week2\share\sample.bin", "D:sample.bin"):
                with self.subTest(name=name):
                    with self.connect() as conn:
                        conn.execute("UPDATE files SET file_name=%s", (name,))
                    directory = rpc(server.server_address, {"type": "get_peers", "file_id": self.metadata["file_id"]})
                    listing = rpc(server.server_address, {"type": "list_files"})
                    self.assertEqual(directory["file"]["file_name"], "sample.bin")
                    self.assertEqual(listing["files"][0]["file_name"], "sample.bin")
                    self.assertNotIn("private-week2", json.dumps([directory, listing]))
                    with self.connect() as conn:
                        self.assertEqual(conn.execute("SELECT file_name FROM files").fetchone()["file_name"], name)

    def test_week2_concurrent_mixed_directory_requests_preserve_integrity(self):
        self.request("register_file", **self.metadata)
        for name in ("shared", "other-0", "other-1", "other-2"):
            self.peer(name)
        self.peer("inactive", [0, 1, 2])
        with self.connect() as conn:
            conn.execute("UPDATE peers SET status='inactive' WHERE peer_id='inactive'")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "unowned.bin"
            path.write_bytes(b"other data")
            unowned = build_metadata(path, 4)
        live_connections = set()
        counts = {"peak": 0, "opened": 0}
        lock = threading.Lock()

        @contextmanager
        def observed_connection():
            pid = None
            try:
                with self.connect() as conn:
                    pid = conn.info.backend_pid
                    with lock:
                        self.assertNotIn(pid, live_connections)
                        live_connections.add(pid)
                        counts["opened"] += 1
                        counts["peak"] = max(counts["peak"], len(live_connections))
                    yield conn
            finally:
                with lock:
                    live_connections.discard(pid)

        self.directory._connect = observed_connection
        gate = threading.Barrier(12)
        with running_server(self.directory, max_clients=24, cleanup_interval=0.1) as server:
            def client(i):
                gate.wait(timeout=10)
                action = i % 6
                if action == 0:
                    payload = {"type": "register_file", **self.metadata}
                elif action == 1:
                    payload = {"type": "announce_chunks", "peer_id": "shared", "file_id": self.metadata["file_id"],
                               "chunk_indexes": [2, 0, 1, 0]}
                elif action == 2:
                    index = (i // 6) % 3
                    payload = {"type": "announce_chunks", "peer_id": f"other-{index}",
                               "file_id": self.metadata["file_id"], "chunk_indexes": [index, index]}
                elif action == 3:
                    payload = {"type": "get_peers", "file_id": self.metadata["file_id"], "chunk_indexes": [2, 0]}
                elif action == 4:
                    payload = {"type": "list_files"}
                else:
                    payload = {"type": "register_file", **unowned}
                response = rpc(server.server_address, payload)
                self.assertTrue(response["ok"], response)
                if action == 3:
                    self.assertEqual(response["file"], self.metadata)
                    self.assertEqual(len(response["peers"]), len({p["peer_id"] for p in response["peers"]}))
                    for peer in response["peers"]:
                        self.assertNotEqual(peer["peer_id"], "inactive")
                        self.assertTrue(set(peer["chunk_indexes"]) <= {0, 2})
                elif action == 4:
                    ids = [f["file_id"] for f in response["files"]]
                    self.assertEqual(len(ids), len(set(ids)))
                    self.assertTrue(set(ids) <= {self.metadata["file_id"]})
            with ThreadPoolExecutor(max_workers=12) as pool:
                futures = [pool.submit(client, i) for i in range(96)]
                for future in futures:
                    future.result(timeout=20)
            result = rpc(server.server_address, {"type": "get_peers", "file_id": self.metadata["file_id"]})
            expected = {"shared": [0, 1, 2], "other-0": [0], "other-1": [1], "other-2": [2]}
            self.assertEqual({p["peer_id"]: p["chunk_indexes"] for p in result["peers"]}, expected)
            self.assertEqual(len(rpc(server.server_address, {"type": "list_files"})["files"]), 1)
        self.assertFalse(live_connections)
        self.assertGreater(counts["peak"], 1)
        self.assertLessEqual(counts["peak"], 25)  # 24 client slots plus the existing cleaner.
        self.assertGreaterEqual(counts["opened"], 98)
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM files").fetchone()["n"], 2)
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM chunks").fetchone()["n"], 6)
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM peer_chunks").fetchone()["n"], 9)
            duplicates = conn.execute("""SELECT peer_id,file_id,chunk_index FROM peer_chunks
                                      GROUP BY peer_id,file_id,chunk_index HAVING count(*)>1""").fetchall()
            self.assertEqual(duplicates, [])

    def test_week2_concurrent_conflicting_registrations_do_not_change_hashes(self):
        self.request("register_file", **self.metadata)
        conflicting = deepcopy(self.metadata)
        conflicting["chunks"][1]["chunk_hash"] = "f" * 64
        gate = threading.Barrier(8)
        with running_server(self.directory) as server:
            def register(i):
                gate.wait(timeout=10)
                metadata = self.metadata if i % 2 == 0 else conflicting
                result = rpc(server.server_address, {"type": "register_file", **metadata})
                if i % 2 == 0:
                    self.assertTrue(result["ok"])
                else:
                    self.assertEqual(result["error_code"], "INVALID_REQUEST")
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(register, i) for i in range(16)]
                for future in futures:
                    future.result(timeout=15)
            result = rpc(server.server_address, {"type": "get_peers", "file_id": self.metadata["file_id"]})
            self.assertEqual(result["file"], self.metadata)
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM chunks").fetchone()["n"], 3)
