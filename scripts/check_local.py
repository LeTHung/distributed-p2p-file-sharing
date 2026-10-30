"""Run local Tracker + Seeder + controlled chunk probe (not the real Leecher)."""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

from dotenv import dotenv_values

from common.framing import ProtocolError, recv_json, send_json
from peer.uploader import tracker_request

ROOT = Path(__file__).resolve().parent.parent


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    if not (ROOT / ".env").exists():
        raise SystemExit("Run python -m scripts.setup_local first.")
    config = {key: value for key, value in dotenv_values(ROOT / ".env").items() if value is not None}
    if config.get("POSTGRES_HOST") not in ("localhost", "127.0.0.1"):
        raise SystemExit("This check only supports PostgreSQL on localhost.")
    # Override TTL only for these child processes until Seeder heartbeat is implemented.
    # Existing services and saved .env are not modified.
    tracker_port, seeder_port = available_port(), available_port()
    while seeder_port == tracker_port:
        seeder_port = available_port()
    env = {**os.environ, **config, "PYTHONPATH": str(ROOT), "TRACKER_HOST": "127.0.0.1",
           "TRACKER_PORT": str(tracker_port), "PEER_TTL_SECONDS": "3600"}
    folder = ROOT / "data" / "local-check"
    folder.mkdir(parents=True, exist_ok=True)
    source = folder / "sample.bin"
    content = bytes(range(256)) * 33 + b"short-tail"
    source.write_bytes(content)
    file_id = hashlib.sha256(content).hexdigest()
    processes, logs = [], []
    peer_id = "local-check-" + str(os.getpid())
    tracker = ("127.0.0.1", tracker_port)

    def start(name, args):
        log = (folder / (name + ".log")).open("w", encoding="utf-8")
        logs.append(log)
        process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    def ready(process, payload, predicate):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Child stopped; see logs in data/local-check")
            try:
                # Startup retries are expected; don't print uploader failure traces.
                request_id = str(uuid.uuid4())
                with socket.create_connection(tracker, timeout=1) as connection:
                    send_json(connection, {**payload, "request_id": request_id})
                    response = recv_json(connection, timeout=1)
                if (response.get("request_id") == request_id and
                        response.get("ok") is True and predicate(response)):
                    return
            except (OSError, EOFError, ProtocolError):
                pass
            time.sleep(0.1)
        raise TimeoutError("Child did not become ready; see data/local-check logs")

    try:
        server = start("tracker", ["-m", "tracker.server"])
        ready(server, {"type": "list_files"}, lambda response: response["ok"])
        seeder = start("seeder", ["-m", "peer.uploader", str(source), "--peer-id", peer_id,
                                  "--port", str(seeder_port), "--chunk-size", "1024"])
        ready(seeder, {"type": "get_peers", "file_id": file_id},
              lambda response: any(peer["peer_id"] == peer_id for peer in response["peers"]))
        probe = subprocess.run([sys.executable, str(ROOT / "tests/fixtures/chunk_probe.py"),
                                "--tracker-port", str(tracker_port), "--file-id", file_id],
                               cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
        if probe.returncode:
            (folder / "probe-error.log").write_text(probe.stderr, encoding="utf-8")
            raise RuntimeError("Chunk probe failed; see data/local-check/probe-error.log")
        result = json.loads(probe.stdout)
        (folder / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
        print("Local PostgreSQL + Tracker + Seeder + controlled probe: OK.")
        print("The probe does not replace Student 3's Leecher or prove multi-source failover.")
    finally:
        # Remove only this check's peer. Keep the reusable sample metadata.
        if processes and processes[0].poll() is None:
            try:
                tracker_request(tracker, {"type": "unregister_peer", "peer_id": peer_id}, 2)
            except (OSError, EOFError, ProtocolError):
                pass
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
