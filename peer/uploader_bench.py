"""Run a local TCP Seeder workload and save verifiable JSON/log evidence.

Usage: python -m peer.uploader_bench --clients 16 --requests 128
This isolates the uploader; it does not benchmark Tracker or a real LAN.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
from pathlib import Path
import socket
import statistics
import threading
import time
import uuid

from common.framing import ProtocolError, recv_exact, recv_json, send_json
from peer.uploader import SharedFile, UploaderServer


def request_chunk(address, shared, index, timeout):
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    expected_length = min(shared.chunk_size, shared.file_size - index * shared.chunk_size)
    result = {"request_id": request_id, "chunk_index": index, "verified": False, "bytes": 0}
    try:
        with socket.create_connection(address, timeout=timeout) as connection:
            send_json(connection, {"type": "get_chunk", "request_id": request_id,
                                   "file_id": shared.file_id, "chunk_index": index})
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("Request deadline exceeded")
            header = recv_json(connection, timeout=remaining)
            if (header.get("ok") is not True or header.get("request_id") != request_id or
                    header.get("file_id") != shared.file_id or type(header.get("chunk_index")) is not int or
                    header["chunk_index"] != index or type(header.get("data_length")) is not int or
                    header["data_length"] != expected_length):
                raise ProtocolError("Unexpected get_chunk response")
            data = recv_exact(connection, expected_length, deadline=started + timeout)
        digest = hashlib.sha256(data).hexdigest()
        if digest != shared.chunks[index]["chunk_hash"]:
            raise ProtocolError("Chunk SHA-256 mismatch")
        result.update(verified=True, bytes=len(data), sha256=digest)
    except (OSError, EOFError, ProtocolError) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    result["latency_seconds"] = round(time.monotonic() - started, 6)
    return result


def run_benchmark(*, clients=16, requests=128, workers=4, max_pending=64,
                  chunk_size=16384, upload_rate=0, timeout=15,
                  output_dir="data/week4-evidence", source=None):
    for name, value in (("clients", clients), ("requests", requests), ("workers", workers), ("chunk_size", chunk_size)):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if type(max_pending) is not int or max_pending < 0 or type(upload_rate) is not int or upload_rate < 0:
        raise ValueError("max_pending and upload_rate must be nonnegative integers")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive finite number")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    folder = Path(output_dir).resolve() / run_id
    folder.mkdir(parents=True)
    if source is None:
        source = folder / "source.bin"
        # Includes a short final chunk with a stable binary dataset.
        source.write_bytes(bytes(range(256)) * 1024 + b"short-tail")
    shared = SharedFile(source, chunk_size)
    logger = logging.getLogger("peer.uploader")
    old_level, old_propagate = logger.level, logger.propagate
    handler = logging.FileHandler(folder / "uploader.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    server = thread = None
    try:
        server = UploaderServer(("127.0.0.1", 0), shared, timeout, max_workers=workers,
                                max_pending=max_pending, upload_rate=upload_rate)
        thread = threading.Thread(target=server.serve_forever, name="bench-uploader")
        thread.start()
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=clients, thread_name_prefix="bench-client") as pool:
            # Bounded submission avoids materializing an arbitrary request count
            # in the executor's unbounded internal queue.
            results = []
            for batch in range(0, requests, clients):
                jobs = [pool.submit(request_chunk, server.server_address, shared,
                                    index % len(shared.chunks), timeout)
                        for index in range(batch, min(batch + clients, requests))]
                results.extend(job.result() for job in jobs)
        duration = time.monotonic() - started
    finally:
        try:
            if server:
                server.request_stop()
                if thread and thread.is_alive():
                    server.shutdown()
                server.server_close()
                if thread:
                    thread.join()
        finally:
            logger.removeHandler(handler)
            handler.close()
            logger.setLevel(old_level)
            logger.propagate = old_propagate
    snapshot = server.metrics.snapshot()
    successful = [result for result in results if result["verified"]]
    verified_bytes = sum(result["bytes"] for result in successful)
    latencies = sorted(result["latency_seconds"] for result in results)
    checks = {
        "all_hashes_verified": len(successful) == requests,
        "worker_limit_respected": snapshot["peak_active_connections"] <= workers,
        "capacity_respected": snapshot["peak_in_flight"] <= workers + max_pending,
        "no_connections_left": snapshot["in_flight"] == snapshot["active_connections"] == 0,
        "all_accepted_finished": snapshot["accepted_connections"] == snapshot["finished_connections"],
        "aggregate_rate_respected": not upload_rate or duration + 0.02 >= verified_bytes / upload_rate,
    }
    report = {
        "run_id": run_id, "scope": "loopback TCP uploader workload; no Tracker or real LAN",
        "passed": all(checks.values()), "checks": checks,
        "config": {"clients": clients, "requests": requests, "workers": workers, "max_pending": max_pending,
                   "chunk_size": chunk_size, "upload_rate_bytes_per_second": upload_rate, "timeout": timeout},
        "source": {"file_id": shared.file_id, "bytes": shared.file_size, "chunks": len(shared.chunks)},
        "verified_requests": len(successful), "failed_requests": requests - len(successful),
        "verified_bytes": verified_bytes, "duration_seconds": round(duration, 6),
        "verified_mib_per_second": round(verified_bytes / duration / (1024 * 1024), 6),
        "latency_seconds": {"p50": statistics.median(latencies),
                            "p95": latencies[max(0, math.ceil(len(latencies) * .95) - 1)], "max": max(latencies)},
        "server": snapshot, "requests": results,
    }
    report_path = folder / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report, report_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--requests", type=int, default=128)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-pending", type=int, default=64)
    parser.add_argument("--chunk-size", type=int, default=16384)
    parser.add_argument("--upload-rate", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--output-dir", default="data/week4-evidence")
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    try:
        report, path = run_benchmark(**vars(args))
    except (OSError, ValueError) as error:
        parser.exit(2, f"Benchmark could not run: {error}\n")
    print(json.dumps({"passed": report["passed"], "verified_requests": report["verified_requests"],
                      "failed_requests": report["failed_requests"], "verified_bytes": report["verified_bytes"],
                      "duration_seconds": report["duration_seconds"], "report": str(path)}, indent=2))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
