# BitTorrent Lite demo guide

This guide gives reproducible Tracker and current Week 1 Seeder/Downloader checks, then identifies remaining work for the full group acceptance demo. The merged base includes Student 2's Seeder lifecycle and Student 3's single-chunk Downloader; it does not yet provide parallel whole-file transfer.

## Start Tracker

1. Start PostgreSQL and configure `.env` from `.env.example` with the local database connection, Tracker host/port, 5-second heartbeat period, and 20-second TTL.
2. From the repository root, initialize or upgrade the Tracker schema:

   ```powershell
   .\.venv\Scripts\python.exe -m tracker.server --init-db-only
   ```

   `Directory.initialize()` applies the idempotent SQL in `database/schema.sql`; do not run schema DDL from a peer process. The `.env` application role needs normal table access to `p2p_tracker`.

3. In a separate terminal, start the Tracker:

   ```powershell
   .\.venv\Scripts\python.exe -m tracker.server
   ```

## Run the Student 1 verification suite

Use a disposable PostgreSQL test database and a test role allowed to create/drop a database for the multiprocess E2E test. The test suite creates unique schemas and one uniquely named E2E database, then removes only those test objects.

```powershell
$env:TRACKER_TEST_DSN = 'host=127.0.0.1 port=5432 dbname=tracker_test user=tracker_test'
$env:TRACKER_UPLOADER_SOURCE = (Resolve-Path '.\peer\uploader.py').Path
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

To run only the real Tracker–Seeder–Downloader E2E:

```powershell
$env:TRACKER_TEST_DSN = 'host=127.0.0.1 port=5432 dbname=tracker_test user=tracker_test'
$env:TRACKER_UPLOADER_SOURCE = (Resolve-Path '.\peer\uploader.py').Path
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_tracker_e2e.py -v
```

To run only the real-Uploader expiry/re-registration scenario:

```powershell
$env:TRACKER_TEST_DSN = 'host=127.0.0.1 port=5432 dbname=tracker_test user=tracker_test'
$env:TRACKER_UPLOADER_SOURCE = (Resolve-Path '.\peer\uploader.py').Path
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_tracker_e2e.py -k recovers_after_expired_peer -v
```

To run only T01's truncated-frame checks (no PostgreSQL DSN is required):

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_tracker_tcp.py -k t01 -v
```

Expected: incomplete prefix/payload, malformed JSON, a missing request ID, and client disconnect are logged as `INVALID_REQUEST`. If the socket can still send, the Tracker replies once with `request_id: null`, `ok: false`, `error_code: "INVALID_REQUEST"`, and a short message, then closes the connection. A later valid client request still succeeds.

This file runs two actual-process E2E scenarios. One restarts the Tracker, waits beyond the test TTL while the Seeder heartbeat runs, runs a controlled chunk probe over all chunks, and asks the actual Downloader CLI to fetch the final short chunk. The second expires the registered peer in its disposable test database after observing a real heartbeat, asserts the Tracker's `PEER_NOT_FOUND` response, checks that the actual Uploader process logs that response and re-registers, verifies the full ownership set appears again, downloads one chunk through the actual Downloader CLI, then sends a graceful interrupt and verifies unregister removed the peer and ownership rows.

The recovery test uses a 2-second heartbeat, 8-second peer TTL, and 60-second cleanup interval only inside its isolated E2E process environment. It first waits for `last_seen` to advance from a real Uploader heartbeat and expires the peer immediately afterward. This makes the `PEER_NOT_FOUND` assertion deterministic while preventing background TTL cleanup from racing the Uploader's recovery. The test drops only its uniquely named database after child processes stop.

To run only the shared heartbeat client checks:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_heartbeat_client.py -v
```

To run the PostgreSQL-backed peer heartbeat/TTL lifecycle test:

```powershell
$env:TRACKER_TEST_DSN = 'host=127.0.0.1 port=5432 dbname=tracker_test user=tracker_test'
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_tracker_postgres.py -k peer_heartbeat_sender -v
```

The heartbeat lifecycle test starts a real Tracker TCP listener, registers a peer and file, keeps the peer alive past a short test TTL, stops the heartbeat sender, then confirms expiry removes the peer and ownership rows without deleting file/chunk metadata.

## Run each current process

Start PostgreSQL using the local PostgreSQL service/cluster for the group. Configure `.env` using `.env.example`, create the `p2p_tracker` database and application role, then run the schema command and Tracker from the repository root:

```powershell
.\.venv\Scripts\python.exe -m tracker.server --init-db-only
.\.venv\Scripts\python.exe -m tracker.server
```

In another terminal, start one actual Seeder/Uploader. Its log prints `file_id`, listening endpoint, and chunk count; keep the source file unchanged while it is sharing:

```powershell
.\.venv\Scripts\python.exe -m peer.uploader .\samples\README.md --peer-id seed-a --port 7001 --chunk-size 16
```

Use the `file_id` printed by the Seeder to run the current actual Downloader CLI:

```powershell
.\.venv\Scripts\python.exe -m peer.downloader <file_id> --chunk-index 0 --output-dir downloads
```

Expected result: the Downloader prints a successful validation and writes `downloads/<file_id>/0.part`; its length and SHA-256 match chunk 0. This command downloads one chunk per invocation. It is not a full-file or parallel-download demo.

Normal Seeder shutdown unregisters the peer, immediately removing its ownership rows. A hard process loss cannot unregister, so `get_peers`/`list_files` stop exposing the peer at TTL expiry, and the background cleaner later cascades deletion to `peer_chunks`. File and chunk records remain. The PostgreSQL lifecycle tests cover this path with a shortened TTL; avoid forcibly stopping a shared demo Seeder unless the group is deliberately demonstrating crash cleanup.

## Re-registration behavior

If a heartbeat returns `PEER_NOT_FOUND`, the peer owner must repeat `register_peer`, `register_file` for each shared file, and `announce_chunks` for all locally owned indexes. Do not assume the Tracker restored ownership: rejoining an expired peer intentionally clears its old `peer_chunks`. Student 2's current `SeederRegistration` performs the sequence after an RPC error on its next heartbeat cycle. For other peer roles, `PeerHeartbeat.on_error` receives a typed error on its worker thread; enqueue a recovery job in the peer's lifecycle manager, and keep long RPC sequences off that callback thread.

## What the automated E2E actually proves

With both test environment variables set, `test_tracker_e2e.py` launches the real Tracker and current checked-in Uploader in separate processes. The restart scenario verifies heartbeat continuity beyond TTL. The recovery scenario forces expiry, observes `PEER_NOT_FOUND` in the real Uploader process, verifies its re-registration and re-announcement in PostgreSQL-backed Tracker results, uses the actual Downloader CLI for one chunk, and verifies normal shutdown unregister. `tests/fixtures/chunk_probe.py` is a controlled test client used by the restart scenario to request and verify all chunks; it is not the Downloader implementation. The actual Downloader assertion in each scenario checks one chunk. These tests do not demonstrate parallel multi-peer scheduling or complete-file reassembly.

T01 is aligned with the test plan and the null `request_id` rule in the group protocol: the Tracker logs `INVALID_REQUEST`, attempts one error response when possible, and closes the connection. See [PROTOCOL.md](PROTOCOL.md) for the precise behavior and handled send-failure case.

## Latest verification record

On 9 November 2026, after the T01 fix, the full suite passed **80 tests, 0 failed, 0 skipped in 64.000 seconds** with PostgreSQL and the checked-in Uploader process configured. Earlier full-suite milestones remain: 76/76 before T01, 75/75 before the heartbeat shutdown-timeout fix, 74/74 before the expiry/recovery E2E, and 70/70 as an earlier historical milestone. The latest focused reruns passed: T01 5/5 in 1.008 seconds, Tracker TCP 16/16 in 3.168 seconds, and heartbeat client 8/8 in 4.474 seconds. The PostgreSQL suite passed 21/21 in 27.732 seconds and both real-process E2Es passed 2/2 in 25.714 seconds in the focused regression run; the final full-suite run reran all cases successfully.

The recovery scenario's test-only environment uses a 2-second heartbeat, 8-second TTL, and 60-second cleanup period. It uses a separate disposable database, verifies a real Uploader process receives `PEER_NOT_FOUND`, re-registers and re-announces all indexes, confirms the actual Downloader CLI retrieves one chunk, then verifies graceful unregister. It is not evidence of parallel or complete-file Downloader behavior.

## Full group demo gate

The current E2E proves one chunk can be retrieved through the merged Tracker, Seeder lifecycle, and Downloader. For final acceptance, extend this to two Seeders and a multi-chunk Leecher run:

1. Start PostgreSQL and the Tracker.
2. Start two Seeders with different chunk ownership for the same metadata and verify they both appear in `get_peers`.
3. Start the Leecher and show requests for different chunks going to both Seeder ports at the same time.
4. Show each chunk hash and the final file SHA-256 matching the source.
5. Stop one Seeder during a second download. Show a timeout, a Tracker refresh, and successful fallback to the remaining Seeder.
6. Wait for the configured TTL and show the stopped Seeder absent from `get_peers`, with its `peer_chunks` removed and file metadata retained.

The Week 1 Downloader currently fetches one chunk per invocation, and Student 2's separate Week 2 worker-pool branch is not in this merged base. Label the multi-peer scenario as planned until those later-week features are integrated and exercised. Do not present the single-chunk E2E as evidence of parallel whole-file transfer or mid-transfer failover.
