"""Actual PostgreSQL + processes: interrupt upload, fail over, then share back."""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from test_tracker_e2e import free_port
from test_tracker_tcp import rpc

ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(os.getenv("TRACKER_TEST_DSN"), "Set TRACKER_TEST_DSN for isolated PostgreSQL E2E")
class WeekThreeE2E(unittest.TestCase):
    def test_kill_during_body_failover_and_download_from_former_leecher(self):
        dsn = os.environ["TRACKER_TEST_DSN"]
        database = "week3_" + uuid.uuid4().hex
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))

        def drop():
            with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
                conn.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)))
        self.addCleanup(drop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        folder = Path(temporary.name)
        logs = Path(os.getenv("TRACKER_TEST_LOG_DIR", str(folder)))
        logs.mkdir(parents=True, exist_ok=True)
        config = conninfo_to_dict(dsn)
        ports = set()
        while len(ports) < 4:
            ports.add(free_port())
        tracker_port, port_a, port_b, peer_port = sorted(ports)
        env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONIOENCODING": "utf-8",
               "POSTGRES_HOST": config.get("host", "127.0.0.1"), "POSTGRES_PORT": config.get("port", "5432"),
               "POSTGRES_DB": database, "POSTGRES_USER": config.get("user", ""),
               "POSTGRES_PASSWORD": config.get("password", ""), "TRACKER_PORT": str(tracker_port),
               "TRACKER_HOST": "127.0.0.1", "HEARTBEAT_INTERVAL_SECONDS": "1", "PEER_TTL_SECONDS": "6",
               "SOCKET_TIMEOUT_SECONDS": "2", "TRACKER_CLEANUP_INTERVAL_SECONDS": "1"}
        source = folder / "source.bin"
        content = bytes(range(256)) * 1024 + b"tail"
        source.write_bytes(content)
        file_id = hashlib.sha256(content).hexdigest()
        address = ("127.0.0.1", tracker_port)

        def start(name, args, console=False):
            output = (logs / (name + ".log")).open("w", encoding="utf-8")
            self.addCleanup(output.close)
            process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env,
                                       creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" and console else 0,
                                       stdout=output, stderr=subprocess.STDOUT)
            def stop():
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            self.addCleanup(stop)
            return process

        def wait(predicate, *processes):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                for process in processes:
                    self.assertIsNone(process.poll(), "Child exited; inspect week3 process logs")
                try:
                    if predicate():
                        return
                except (OSError, EOFError):
                    pass
                time.sleep(0.05)
            self.fail("Timed out; inspect week3 process logs")

        tracker = start("week3-tracker", ["-m", "tracker.server"])
        wait(lambda: rpc(address, {"type": "list_files"})["ok"], tracker)
        query = {"type": "get_peers", "file_id": file_id}
        seeder_a = start("week3-seeder-a", ["-m", "peer.uploader", str(source), "--peer-id", "aaa-first",
                          "--port", str(port_a), "--chunk-size", "65536", "--upload-rate", "65536"])
        seeder_b = start("week3-seeder-b", ["-m", "peer.uploader", str(source), "--peer-id", "bbb-backup",
                          "--port", str(port_b), "--chunk-size", "65536", "--upload-rate", "262144"])
        wait(lambda: len(rpc(address, query).get("peers", [])) == 2, tracker, seeder_a, seeder_b)
        runner = "import runpy,sys,signal; "
        if os.name == "nt":
            runner += "signal.signal(signal.SIGBREAK,lambda s,f: (_ for _ in ()).throw(KeyboardInterrupt())); "
        runner += "sys.argv=sys.argv[1:];runpy.run_module('peer.downloader',run_name='__main__')"
        leecher = start("week3-sharing-leecher", ["-c", runner, "peer.downloader", file_id,
                        "--tracker-port", str(tracker_port), "--workers", "2", "--peer-id", "leecher-share",
                        "--serve-port", str(peer_port), "--output-dir", str(folder / "download")], console=True)
        wait(lambda: "Upload started chunk=0" in (logs / "week3-seeder-a.log").read_text(encoding="utf-8"),
             tracker, seeder_a, seeder_b, leecher)
        # Header has been sent, but the paced 64KiB body cannot finish yet.
        self.assertNotIn("Sent chunk=0", (logs / "week3-seeder-a.log").read_text(encoding="utf-8"))
        seeder_a.terminate()
        seeder_a.wait(timeout=10)
        final = folder / "download" / file_id / "completed" / source.name
        wait(lambda: final.exists() and final.read_bytes() == content, tracker, seeder_b, leecher)
        text = (logs / "week3-sharing-leecher.log").read_text(encoding="utf-8")
        self.assertIn("[FAILOVER]", text)
        self.assertIn("bbb-backup", text)
        wait(lambda: any(p["peer_id"] == "leecher-share" and p["chunk_indexes"] == [0, 1, 2, 3, 4]
                         for p in rpc(address, query)["peers"]), tracker, leecher)
        seeder_b.terminate()
        seeder_b.wait(timeout=10)
        time.sleep(7)
        peers = rpc(address, query)["peers"]
        self.assertEqual([p["peer_id"] for p in peers], ["leecher-share"])
        followup = subprocess.run([sys.executable, "-m", "peer.downloader", file_id,
                                  "--tracker-port", str(tracker_port), "--output-dir", str(folder / "followup")],
                                 cwd=ROOT, env=env, capture_output=True, timeout=30)
        (logs / "week3-followup-leecher.log").write_bytes(followup.stdout + followup.stderr)
        self.assertEqual(followup.returncode, 0)
        self.assertEqual((folder / "followup" / file_id / "completed" / source.name).read_bytes(), content)
        leecher.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
        leecher.wait(timeout=15)
        self.assertEqual(leecher.returncode, 0)
        self.assertEqual(rpc(address, query)["peers"], [])
        (logs / "week3-result.json").write_text(json.dumps({"verified": True, "bytes": len(content),
            "file_id": file_id, "killed_mid_body": True, "failover": True,
            "downloaded_from_former_leecher": True, "ttl_and_unregister": True}), encoding="utf-8")
