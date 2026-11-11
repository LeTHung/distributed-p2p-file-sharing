"""Optional multiprocess test against the actual uploader (including another branch).

Set TRACKER_TEST_DSN and TRACKER_UPLOADER_SOURCE. The test DB role needs
CREATEDB; a unique database created by this test is the only database removed.
"""

import hashlib
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from common.framing import ProtocolError
from common.metadata import build_metadata
from test_tracker_tcp import rpc

ROOT = Path(__file__).resolve().parent.parent
UPLOADER = os.getenv("TRACKER_UPLOADER_SOURCE")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _rpc_if_available(address, payload):
    try:
        return rpc(address, payload)
    except (OSError, EOFError, ProtocolError):
        return None


@unittest.skipUnless(os.getenv("TRACKER_TEST_DSN") and UPLOADER,
                     "Set TRACKER_TEST_DSN and TRACKER_UPLOADER_SOURCE for actual-uploader integration")
class MultiProcessTests(unittest.TestCase):
    def test_real_uploader_recovers_after_expired_peer_and_unregisters(self):
        dsn = os.environ["TRACKER_TEST_DSN"]
        database = "tracker_recovery_" + uuid.uuid4().hex
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))

        def drop_database():
            with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
                conn.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)))

        self.addCleanup(drop_database)
        tracker_port, seeder_port = free_port(), free_port()
        while seeder_port == tracker_port:
            seeder_port = free_port()
        peer_id = "recovery-" + uuid.uuid4().hex[:10]
        config = conninfo_to_dict(dsn)
        test_dsn = make_conninfo(dsn, dbname=database)
        env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONIOENCODING": "utf-8",
               "POSTGRES_HOST": config.get("host", "127.0.0.1"),
               "POSTGRES_PORT": config.get("port", "5432"), "POSTGRES_DB": database,
               "POSTGRES_USER": config.get("user", ""), "POSTGRES_PASSWORD": config.get("password", ""),
               "TRACKER_HOST": "127.0.0.1", "TRACKER_PORT": str(tracker_port),
               "SOCKET_TIMEOUT_SECONDS": "2", "HEARTBEAT_INTERVAL_SECONDS": "2",
               "PEER_TTL_SECONDS": "8", "TRACKER_CLEANUP_INTERVAL_SECONDS": "60"}
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        source = Path(folder.name) / "recovery-source.bin"
        data = bytes(range(251)) * 13 + b"short-tail"
        source.write_bytes(data)
        metadata = build_metadata(source, 1024)
        log_lines = {}

        def start(name, args):
            options = {}
            if os.name == "nt":
                options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, encoding="utf-8", errors="replace", bufsize=1, **options)
            log_lines[name] = []

            def capture_output():
                for line in process.stdout:
                    log_lines[name].append(line)
                process.stdout.close()

            reader = threading.Thread(target=capture_output, name=f"{name}-log-reader", daemon=True)
            reader.start()

            def stop_if_running():
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=10)
                reader.join(timeout=5)

            self.addCleanup(stop_if_running)
            return process

        def log_text(name):
            return "".join(log_lines.get(name, []))

        def wait_until(predicate, message, process=None, timeout=12):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if process is not None and process.poll() is not None:
                    self.fail(f"Process exited before {message}: {log_text('e2e-recovery-uploader')}")
                if predicate():
                    return
                time.sleep(0.05)
            self.fail(f"Timed out waiting for {message}")

        def read_last_seen():
            with psycopg.connect(test_dsn, autocommit=True, connect_timeout=5) as conn:
                return conn.execute("SELECT last_seen FROM peers WHERE peer_id=%s", (peer_id,)).fetchone()

        tracker = start("e2e-recovery-tracker", ["-m", "tracker.server"])
        wait_until(lambda: (lambda result: result and result["ok"])(
            _rpc_if_available(("127.0.0.1", tracker_port), {"type": "list_files"})),
            "Tracker startup", tracker)
        runner = "import runpy,signal,sys; "
        if os.name == "nt":
            runner += ("signal.signal(signal.SIGBREAK, lambda signum, frame: "
                       "(_ for _ in ()).throw(KeyboardInterrupt())); ")
        runner += "target=sys.argv.pop(1); runpy.run_path(target,run_name='__main__')"
        uploader = start("e2e-recovery-uploader", ["-c", runner, UPLOADER, str(source), "--peer-id", peer_id,
                                                     "--port", str(seeder_port), "--chunk-size", "1024"])
        query = {"type": "get_peers", "file_id": metadata["file_id"]}
        wait_until(lambda: (lambda result: result and result.get("ok") and
                            any(peer["peer_id"] == peer_id for peer in result["peers"]))(
            _rpc_if_available(("127.0.0.1", tracker_port), query)),
            "real Uploader registration and initial announce", uploader)
        initial = rpc(("127.0.0.1", tracker_port), query)
        initial_peer = next(peer for peer in initial["peers"] if peer["peer_id"] == peer_id)
        expected_indexes = list(range(metadata["total_chunks"]))
        self.assertEqual(initial_peer["chunk_indexes"], expected_indexes)

        # Observe one real Uploader heartbeat first, then expire its row. This
        # leaves almost a full heartbeat interval to assert PEER_NOT_FOUND before
        # the Uploader's next scheduled request, avoiding a timing race.
        first_seen = read_last_seen()
        self.assertIsNotNone(first_seen, "Uploader must have registered in PostgreSQL")
        baseline = first_seen[0]
        wait_until(lambda: (lambda row: row is not None and row[0] > baseline)(read_last_seen()),
                   "a real Uploader heartbeat", uploader, timeout=6)
        with psycopg.connect(test_dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute("UPDATE peers SET last_seen=clock_timestamp()-INTERVAL '1 hour' WHERE peer_id=%s",
                         (peer_id,))
        expired = rpc(("127.0.0.1", tracker_port), {"type": "heartbeat", "peer_id": peer_id})
        self.assertFalse(expired["ok"])
        self.assertEqual(expired["error_code"], "PEER_NOT_FOUND")

        wait_until(lambda: "PEER_NOT_FOUND" in log_text("e2e-recovery-uploader"),
                   "the actual Uploader process to receive PEER_NOT_FOUND", uploader, timeout=6)
        wait_until(lambda: f"Seeder registered again peer_id={peer_id}" in log_text("e2e-recovery-uploader"),
                   "the actual Uploader to re-register", uploader, timeout=10)
        wait_until(lambda: (lambda result: result and result.get("ok") and
                            any(peer["peer_id"] == peer_id and peer["chunk_indexes"] == expected_indexes
                                for peer in result["peers"]))(
            _rpc_if_available(("127.0.0.1", tracker_port), query)),
            "the Tracker directory to contain the re-announced chunks", uploader, timeout=5)

        # Exercise the real Student 3 CLI only for one verified chunk; this does
        # not assert parallel whole-file transfer or complete downloader failover.
        download_dir = Path(folder.name) / "recovery-download"
        leecher = subprocess.run([sys.executable, "-m", "peer.downloader", metadata["file_id"],
                                  "--chunk-index", str(metadata["total_chunks"] - 1),
                                  "--tracker-port", str(tracker_port), "--output-dir", str(download_dir)],
                                 cwd=ROOT, env=env, capture_output=True, text=True,
                                 encoding="utf-8", errors="replace", timeout=15)
        self.assertEqual(leecher.returncode, 0, f"Actual Downloader CLI failed: {leecher.stdout}\n{leecher.stderr}")
        final_index = metadata["total_chunks"] - 1
        saved = download_dir / metadata["file_id"] / f"{final_index}.part"
        self.assertEqual(saved.read_bytes(), data[final_index * 1024:])

        # A graceful console interrupt unwinds the Uploader context manager and unregisters.
        uploader.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
        uploader.wait(timeout=10)
        self.assertEqual(uploader.returncode, 0, log_text("e2e-recovery-uploader"))
        with psycopg.connect(test_dsn, autocommit=True, connect_timeout=5) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM peers WHERE peer_id=%s", (peer_id,)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM peer_chunks WHERE peer_id=%s", (peer_id,)).fetchone()[0], 0)
        self.assertEqual(rpc(("127.0.0.1", tracker_port), {"type": "list_files"})["files"], [])

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
        env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONIOENCODING": "utf-8",
               "POSTGRES_HOST": config.get("host", "127.0.0.1"),
               "POSTGRES_PORT": config.get("port", "5432"), "POSTGRES_DB": database,
               "POSTGRES_USER": config.get("user", ""), "POSTGRES_PASSWORD": config.get("password", ""),
               "TRACKER_HOST": "127.0.0.1", "TRACKER_PORT": str(tracker_port),
               "SOCKET_TIMEOUT_SECONDS": "3", "HEARTBEAT_INTERVAL_SECONDS": "1", "PEER_TTL_SECONDS": "6"}
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
        # More than a full TTL: the Seeder must remain discoverable through
        # actual heartbeat RPCs before the real Leecher asks for a chunk.
        time.sleep(8)
        live = rpc(("127.0.0.1", tracker_port), query)
        self.assertEqual(len(live["peers"]), 1)
        self.assertEqual(live["file"], metadata)
        probe = subprocess.run([sys.executable, str(ROOT / "tests" / "fixtures" / "chunk_probe.py"),
                                "--tracker-port", str(tracker_port), "--file-id", metadata["file_id"]],
                               cwd=ROOT, env=env, capture_output=True, text=True, timeout=15, check=True)
        report = json.loads(probe.stdout)
        self.assertTrue(report["verified"])
        self.assertEqual(report["bytes"], len(data))
        self.assertEqual(report["file_id"], hashlib.sha256(data).hexdigest())
        (log_dir / "e2e-probe.json").write_text(probe.stdout, encoding="utf-8")
        # Also verify Student 3's actual CLI against PostgreSQL + restarted Tracker
        # + Student 2's actual Seeder, including the short final chunk.
        download_dir = Path(folder.name) / "leecher"
        leecher = subprocess.run([sys.executable, "-m", "peer.downloader", metadata["file_id"],
                                  "--chunk-index", str(metadata["total_chunks"] - 1),
                                  "--tracker-port", str(tracker_port),
                                  "--output-dir", str(download_dir)], cwd=ROOT, env=env,
                                 capture_output=True, timeout=15)
        (log_dir / "e2e-leecher.log").write_bytes(leecher.stdout + leecher.stderr)
        self.assertEqual(leecher.returncode, 0, "Leecher CLI failed; inspect e2e-leecher.log")
        final_index = metadata["total_chunks"] - 1
        saved = download_dir / metadata["file_id"] / f"{final_index}.part"
        self.assertEqual(saved.read_bytes(), data[final_index * 1024:])
        second_port = free_port()
        while second_port in (tracker_port, seeder_port):
            second_port = free_port()
        second = start("e2e-seeder-b", ["-m", "peer.uploader", str(source),
                       "--peer-id", "seeder-b", "--port", str(second_port), "--chunk-size", "1024"])
        ready(second, query, lambda r: r["ok"] and len(r["peers"]) == 2)
        full_output = Path(folder.name) / "two-sources"
        full = subprocess.run([sys.executable, "-m", "peer.downloader", metadata["file_id"],
                               "--tracker-port", str(tracker_port), "--workers", "2",
                               "--output-dir", str(full_output)], cwd=ROOT, env=env,
                              capture_output=True, timeout=30)
        (log_dir / "e2e-week2-full-file.log").write_bytes(full.stdout + full.stderr)
        self.assertEqual(full.returncode, 0, "Full-file CLI failed; inspect e2e-week2-full-file.log")
        self.assertEqual((full_output / metadata["file_id"] / "completed" / source.name).read_bytes(), data)
        log = full.stderr.decode("utf-8")
        success_lines = [line for line in log.splitlines() if ' OK ' in line]
        self.assertTrue(any('actual-uploader' in line for line in success_lines))
        self.assertTrue(any('seeder-b' in line for line in success_lines))
        (log_dir / "week2-result.json").write_text(json.dumps({"verified": True,
            "sources": ["actual-uploader", "seeder-b"], "bytes": len(data),
            "chunks": metadata["total_chunks"], "file_id": metadata["file_id"]}), encoding="utf-8")
