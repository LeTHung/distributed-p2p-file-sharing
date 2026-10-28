"""Real PostgreSQL tests, opt in with TRACKER_TEST_DSN.

Each test creates a unique schema and drops only that owned schema afterward.
No existing tables or schemas are truncated or removed.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
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
        self.request("register_file", **empty)
        self.assertEqual(self.request("get_peers", file_id=empty["file_id"])["chunks"], [])

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
        self.assertEqual(r["chunks"], self.metadata["chunks"])
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
        self.assertEqual(r["chunks"], self.metadata["chunks"])
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
        self.assertEqual(error.exception.code, "CHUNK_NOT_FOUND")
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
