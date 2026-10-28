"""Optional multiprocess test against the actual uploader (including another branch).

Set TRACKER_TEST_DSN and TRACKER_UPLOADER_SOURCE. The test DB role needs
CREATEDB; a unique database created by this test is the only database removed.
"""

import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from common.framing import ProtocolError
from common.metadata import build_metadata
from test_tracker_tcp import rpc

ROOT = Path(__file__).resolve().parent.parent
UPLOADER = os.getenv("TRACKER_UPLOADER_SOURCE")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@unittest.skipUnless(os.getenv("TRACKER_TEST_DSN") and UPLOADER,
                     "Set TRACKER_TEST_DSN and TRACKER_UPLOADER_SOURCE for actual-uploader integration")
class MultiProcessTests(unittest.TestCase):
    def test_tracker_restart_and_real_uploader_verified_by_controlled_probe(self):
        dsn = os.environ["TRACKER_TEST_DSN"]
        database = "tracker_e2e_" + uuid.uuid4().hex
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        def drop_database():
            with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
                conn.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)))
        self.addCleanup(drop_database)
        tracker_port, seeder_port = free_port(), free_port()
        while seeder_port == tracker_port:
            seeder_port = free_port()
        config = conninfo_to_dict(dsn)
        env = {**os.environ, "PYTHONPATH": str(ROOT), "POSTGRES_HOST": config.get("host", "127.0.0.1"),
               "POSTGRES_PORT": config.get("port", "5432"), "POSTGRES_DB": database,
               "POSTGRES_USER": config.get("user", ""), "POSTGRES_PASSWORD": config.get("password", ""),
               "TRACKER_HOST": "127.0.0.1", "TRACKER_PORT": str(tracker_port),
               "SOCKET_TIMEOUT_SECONDS": "3", "HEARTBEAT_INTERVAL_SECONDS": "5", "PEER_TTL_SECONDS": "20"}
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        log_dir = Path(os.getenv("TRACKER_TEST_LOG_DIR", folder.name))
        log_dir.mkdir(parents=True, exist_ok=True)
        source = Path(folder.name) / "sample.bin"
        data = bytes(range(256)) * 33 + b"short-tail"
        source.write_bytes(data)
        metadata = build_metadata(source, 1024)
        # Verify Student 1's metadata matches the actual Student 2 implementation.
        importer = "import runpy,sys,json; m=runpy.run_path(sys.argv[1]); print(json.dumps(m['SharedFile'](sys.argv[2],1024).metadata()))"
        import json
        result = subprocess.run([sys.executable, "-c", importer, UPLOADER, str(source)], cwd=ROOT,
                                env=env, capture_output=True, text=True, timeout=10, check=True)
        self.assertEqual(json.loads(result.stdout), metadata)

        def start(name, args):
            output = (log_dir / (name + ".log")).open("w", encoding="utf-8")
            self.addCleanup(output.close)
            process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env,
                                       stdout=output, stderr=subprocess.STDOUT)
            def stop():
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=10)
            self.addCleanup(stop)
            return process

        def ready(process, payload, predicate):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                self.assertIsNone(process.poll(), "Child process stopped; inspect integration logs")
                try:
                    response = rpc(("127.0.0.1", tracker_port), payload)
                    if predicate(response):
                        return
                except (OSError, EOFError, ProtocolError):
                    pass
                time.sleep(0.05)
            self.fail("Timed out waiting for child process")

        tracker = start("e2e-tracker-before-restart", ["-m", "tracker.server"])
        ready(tracker, {"type": "list_files"}, lambda r: r["ok"])
        runner = "import runpy,sys; target=sys.argv.pop(1); runpy.run_path(target,run_name='__main__')"
        seeder = start("e2e-uploader", ["-c", runner, UPLOADER, str(source), "--peer-id", "actual-uploader",
                                       "--port", str(seeder_port), "--chunk-size", "1024"])
        query = {"type": "get_peers", "file_id": metadata["file_id"]}
        ready(seeder, query, lambda r: r["ok"] and len(r["peers"]) == 1)
        tracker.terminate()
        tracker.wait(timeout=10)
        tracker = start("e2e-tracker-after-restart", ["-m", "tracker.server"])
        ready(tracker, query, lambda r: r["ok"] and len(r["peers"]) == 1)
        probe = subprocess.run([sys.executable, str(ROOT / "tests" / "fixtures" / "chunk_probe.py"),
                                "--tracker-port", str(tracker_port), "--file-id", metadata["file_id"]],
                               cwd=ROOT, env=env, capture_output=True, text=True, timeout=15, check=True)
        report = json.loads(probe.stdout)
        self.assertTrue(report["verified"])
        self.assertEqual(report["bytes"], len(data))
        self.assertEqual(report["file_id"], hashlib.sha256(data).hexdigest())
        (log_dir / "e2e-probe.json").write_text(probe.stdout, encoding="utf-8")
