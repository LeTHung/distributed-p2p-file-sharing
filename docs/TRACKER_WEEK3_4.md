# Tracker week 3 and week 4 handover

This note records the Student 1 Tracker implementation, evidence, and remaining integration work for the BitTorrent-Lite group project.

## Week 3 implementation

The Tracker accepts the existing `heartbeat` request with `request_id` and `peer_id`. Each accepted heartbeat refreshes `peers.last_seen`. The default peer heartbeat period is 5 seconds and the default peer TTL is 20 seconds. The Tracker runs a background cleanup loop, configurable with `TRACKER_CLEANUP_INTERVAL_SECONDS` (default 5 seconds), and deletes only peers whose timestamps have expired.

Peer and directory lookups check both `status='active'` and the TTL boundary. This prevents a peer from appearing in `get_peers` or `list_files` during the interval before the cleanup loop runs. PostgreSQL's `ON DELETE CASCADE` removes that peer's `peer_chunks`; file and chunk metadata is stored separately and remains available.

Each directory operation uses its own PostgreSQL connection context, so successful operations commit and failed operations roll back. Peer registration, heartbeat, announce, and expiry deletion use row locking or PostgreSQL's predicate recheck to avoid reviving stale ownership or deleting a peer whose heartbeat committed while cleanup waited. A returning expired peer must announce its current chunks again. Database failures return `INTERNAL_ERROR`; logs retain request IDs and exception types without exposing database details.

The Tracker implementation and its lifecycle handling were already present on the Week 2 base. Week 3 adds a reusable peer sender in `common/heartbeat.py`, without changing the wire contract or modifying Student 2 or Student 3's owned modules. The updated base also includes Student 2's Seeder lifecycle and Student 3's Week 1 single-chunk Downloader from the fetched `develop` history.

### Shared peer heartbeat sender

Start the helper after a successful `register_peer` call and stop it during normal shutdown:

```python
from common.heartbeat import PeerHeartbeat, TrackerHeartbeatError

def heartbeat_failed(error: TrackerHeartbeatError) -> None:
    if error.error_code == "PEER_NOT_FOUND":
        # The owning peer must register again and announce its current chunks.
        schedule_reregister_and_reannounce()

heartbeat = PeerHeartbeat(
    (settings.tracker_host, settings.tracker_port),
    peer_id,
    interval_seconds=settings.heartbeat_interval_seconds,
    timeout_seconds=settings.socket_timeout_seconds,
    on_error=heartbeat_failed,
)
heartbeat.start()
try:
    serve_peer_requests()
finally:
    heartbeat.stop()
```

The callback runs on the heartbeat worker thread. Peer implementations should hand recovery work to their normal lifecycle/controller path and avoid doing a long re-registration operation inside the callback. A heartbeat does not register a peer or announce ownership on the peer's behalf.

## Week 4 integration and verification

The Tracker logs database failures and unexpected request failures with request IDs, logs response-send failures with the request ID and remote endpoint, and logs incomplete-frame timeouts/disconnects with the remote endpoint and exception type (a request ID is not available until a full frame is read). Background cleanup results and failures are also logged. The database tests exercise concurrent Tracker requests, transaction rollback, a heartbeat/prune race, TTL cleanup, cascade behavior, and metadata preservation.

Verification evidence:

- Baseline before changes, without the optional database environment: 31 passed, 20 skipped. The skips were PostgreSQL and actual-Uploader integration tests.
- Real PostgreSQL baseline using the repository's current Uploader module: 51 passed, 0 failed, 0 skipped in 30.228 seconds.
- Focused heartbeat sender tests: 4 passed.
- Focused real PostgreSQL heartbeat lifecycle test: 1 passed. A live peer stayed discoverable for longer than a 1-second test TTL; after stopping the sender, cleanup removed the peer and peer-chunk rows while retaining the file and all three chunk records.
- Actual Student 2 Week 2 Uploader source from commit `d728cea`: the existing Tracker restart and controlled chunk-probe E2E test passed separately in 5.744 seconds. The probe retrieved and verified the source file's chunks.
- Tracker TCP regression suite after transport logging changes: 12 passed, 0 failed.
- Final full suite with PostgreSQL and that Uploader source: 57 passed, 0 failed, 0 skipped in 33.852 seconds. This includes the real-Uploader E2E test.
- After integrating `origin/develop` into Week 2, the complete merged suite passed 64 tests, 0 failures, 0 skips in 40.718 seconds. This includes PostgreSQL concurrency/TTL tests, Seeder lifecycle tests, Downloader tests, and the real Tracker–Seeder–Leecher single-chunk E2E after Tracker restart.
- The Week 3–4 branch was fast-forwarded to the same merged base and the final full suite passed 70 tests, 0 failures, 0 skips in 49.612 seconds. This includes the shared heartbeat client, real PostgreSQL heartbeat/TTL lifecycle, Student 2 Seeder lifecycle, Student 3 Downloader tests, and the actual Tracker–Seeder–Downloader E2E.

The project rubric does not specify a numeric throughput or latency target, so no performance benchmark or synthetic performance result is claimed.

## Integration status and blockers

The merged Student 2 Seeder lifecycle sends heartbeats, retries registration after heartbeat failure, and unregisters during normal shutdown. Lifecycle tests and the real E2E passed. Student 2's separate Week 2 worker-pool branch, commit `d728cea`, is still not part of `develop` or the Week 2 merge; its bounded concurrent uploader/throttling work remains a separate integration item. Student 2's current module uses `SeederRegistration`; the shared `PeerHeartbeat` helper remains available to other peer roles.

Student 3's Week 1 Downloader is now present, and the real E2E verifies one short final chunk after Tracker restart while the Seeder remains alive beyond a 6-second test TTL. The CLI downloads one chunk per invocation. Parallel scheduling, whole-file reassembly, announcements of verified downloaded chunks, and a multi-chunk mid-transfer failure demo remain later-week integration work and have not been verified here.

The current E2E result covers Tracker, Seeder heartbeat, and one Downloader chunk request. It is not evidence of parallel whole-file transfer or mid-transfer failover. No protocol fields or error codes changed; the existing TCP/JSON API contract remains authoritative.

## Handover checklist

- Student 2: integrate the separate Week 2 bounded worker-pool branch and demonstrate simultaneous chunk uploads; retain the merged registration/heartbeat/unregister lifecycle.
- Student 3: extend the Week 1 single-chunk CLI to parallel multi-chunk scheduling, whole-file reassembly, verified chunk announcements, and the planned failure/retry behavior.
- Integration owner: merge the reviewed Tracker, Uploader, and Downloader branches into `develop`, then run the full test suite and the four-week demo plan from a clean checkout.
- Before submission: attach real logs/video for two Seeder sources, parallel chunk requests, complete-file SHA-256 equality, Seeder failure/failover, and Tracker TTL cleanup.

## Audited Tracker contract and peer lifecycle

The wire contract remains one JSON request and one JSON response per TCP connection. Both are UTF-8 JSON objects framed by a four-byte unsigned big-endian byte length (`common.framing`). The Tracker never sends chunk bytes. A request uses a fresh `request_id`; the response echoes it. Examples below show the field shape; replace the abbreviated IDs and hashes with real values before sending them.

```json
{"type":"register_peer","request_id":"req-1","peer_id":"seed-a","host":"127.0.0.1","port":7001}
```

```json
{"request_id":"req-1","ok":true,"peer_id":"seed-a"}
```

File registration supplies immutable file/chunk metadata. The file ID is the lowercase SHA-256 of the entire file; each chunk hash is SHA-256 of that chunk.

```json
{"type":"register_file","request_id":"req-2","file_id":"<64-lowercase-hex>","file_name":"demo.bin","file_size":5,"chunk_size":4,"total_chunks":2,"chunks":[{"chunk_index":0,"chunk_hash":"<64-lowercase-hex>"},{"chunk_index":1,"chunk_hash":"<64-lowercase-hex>"}]}
```

`announce_chunks` takes `peer_id`, `file_id`, and `chunk_indexes`; it is additive and idempotent. `heartbeat` takes `peer_id`. `unregister_peer` takes `peer_id` and removes that peer's ownership. Each successful request returns `ok: true` and the relevant peer/file IDs.

| `type` | Required fields beyond `type` and `request_id` | Success response fields |
|---|---|---|
| `register_peer` | `peer_id`, unicast IP `host`, TCP `port` | `peer_id` |
| `register_file` | `file_id`, `file_name`, `file_size`, `chunk_size`, `total_chunks`, ordered `chunks` | `file_id` |
| `announce_chunks` | `peer_id`, `file_id`, `chunk_indexes` | `peer_id`, `file_id` |
| `get_peers` | `file_id`; optional `peer_id`, `chunk_indexes` | complete file metadata and filtered `peers` |
| `heartbeat` | `peer_id` | `peer_id` |
| `unregister_peer` | `peer_id` | `peer_id` |
| `list_files` | none | `files` |

`get_peers` takes `file_id`, and optionally `chunk_indexes` and `peer_id` (the latter excludes that peer from results). It returns active sources and complete file metadata, even when the source list is filtered to a subset of chunks:

```json
{"request_id":"req-3","ok":true,"file_id":"<64-lowercase-hex>","file":{"file_id":"<64-lowercase-hex>","file_name":"demo.bin","file_size":5,"chunk_size":4,"total_chunks":2,"chunks":[{"chunk_index":0,"chunk_hash":"<64-lowercase-hex>"},{"chunk_index":1,"chunk_hash":"<64-lowercase-hex>"}]},"peers":[{"peer_id":"seed-a","host":"127.0.0.1","port":7001,"chunk_indexes":[0,1]}]}
```

For compatibility, the current response also contains the metadata fields at the top level. `list_files` returns each file once when at least one nonexpired peer owns a chunk. Errors use `ok: false`, `error_code`, and a safe `message`; `PEER_NOT_FOUND` tells the owner to register again and announce ownership again. Database exception details are logged server-side by exception class/request ID and are not returned to clients.

### Recovery ownership

`PeerHeartbeat` reports `PEER_NOT_FOUND` distinctly from `TRACKER_UNAVAILABLE`, `INVALID_RESPONSE`, and other Tracker error codes. Transport failures are retried on the next configured interval using a new request ID. The callback runs on the sender thread, is protected from raising into the application, and should enqueue recovery work. The peer's lifecycle manager owns re-registration because only it knows the advertised host/port and the complete file/chunk metadata. The required sequence is `register_peer` → `register_file` for each shared file → `announce_chunks` for every currently owned chunk. Until those announcements complete, the peer must not assume its old ownership has been restored.

Student 2's checked-in `SeederRegistration` already performs that sequence after a failed heartbeat RPC and unregisters during normal shutdown. It uses the existing API directly and is not silently replaced by the shared helper. A hard process loss does not run unregister; TTL cleanup handles it. Student 3's checked-in Downloader is still a one-chunk client rather than a serving peer, so it currently has no peer registration or heartbeat lifecycle.

### Transaction and concurrency guarantees

- Every Tracker RPC owns one PostgreSQL connection/transaction. Context exit commits success and rolls back a raised validation or SQL error; an invalid batch cannot leave partial `peer_chunks` rows.
- `register_file` is idempotent for the same metadata and rejects reused file IDs with different sizes, chunk layout, or hashes. A conflict rolls back the entire registration attempt.
- `announce_chunks` locks and verifies a live peer before writing ownership. `register_peer` locks the peer row and clears old ownership after expiry or endpoint change.
- `heartbeat` updates `last_seen` only while the row is active and still within the TTL. A stale peer gets `PEER_NOT_FOUND` and must perform full registration/announcement recovery.
- Background cleanup deletes rows at `last_seen <= now - TTL`. PostgreSQL rechecks the delete predicate after a row-lock wait, so a heartbeat that commits while cleanup is waiting is retained. The peer foreign key cascades to `peer_chunks`; `files` and `chunks` are independent and remain.
- `get_peers` and `list_files` apply active-status and TTL predicates themselves, so stale entries are hidden even before background cleanup runs.
- The TCP server uses bounded request threads (`TRACKER_MAX_CLIENTS`), per-connection socket deadlines and one framed RPC per connection. Malformed clients and failed DB calls receive a protocol error where possible and do not stop the listener.

## Current commands and verification map

From the repository root, configure `.env` from `.env.example`, create the application database/role, then run:

```powershell
.\.venv\Scripts\python.exe -m tracker.server --init-db-only
.\.venv\Scripts\python.exe -m tracker.server
.\.venv\Scripts\python.exe -m peer.uploader .\samples\README.md --peer-id seed-a --port 7001 --chunk-size 1024
.\.venv\Scripts\python.exe -m peer.downloader <file_id-from-seeder-log> --chunk-index 0 --output-dir downloads
```

For real PostgreSQL and multiprocess E2E tests, use a disposable test database and a role with `CREATEDB`, then set `TRACKER_TEST_DSN` and `TRACKER_UPLOADER_SOURCE` as shown in `docs/DEMO_GUIDE.md`. The full suite contains unit/framing/validation tests, real PostgreSQL transaction/concurrency/TTL tests, real TCP Tracker tests, and an E2E that launches the actual checked-in Uploader process and actual Downloader CLI. The E2E's `chunk_probe.py` is an additional controlled test client, not a replacement for either peer module. The real Downloader assertion fetches one final short chunk; it does not test parallel whole-file assembly.

Student 2's separate Week 2 bounded worker-pool change and Student 3's parallel multi-chunk scheduling, whole-file reassembly, verified chunk announcements, and mid-transfer retry/failover remain outside this Tracker branch. Do not claim the planned multi-peer full-file demo until those modules are integrated and their acceptance scenario has been run.

## Final completion audit — 8 November 2026

The earlier full-suite milestones remain in the history above: 70 passed was the first full-suite result on the Week 3–4 base, then 74 passed was the latest run before this completion work. The final run after adding the process-recovery E2E contains **75 passed, 0 failed, 0 skipped in 60.909 seconds**.

### Actual Uploader expiry/recovery E2E

`tests/test_tracker_e2e.py::MultiProcessTests.test_real_uploader_recovers_after_expired_peer_and_unregisters` launches the checked-in `peer/uploader.py` as its own process and a real Tracker process against a uniquely named disposable PostgreSQL database. The test waits until the real Uploader has sent a heartbeat, moves its `last_seen` into the past, confirms a Tracker heartbeat returns `PEER_NOT_FOUND`, and observes that error in the Uploader process log. The process then runs its existing lifecycle: `register_peer`, `register_file`, and `announce_chunks`. The test verifies the peer and every chunk index are visible again, then invokes the actual `peer.downloader` CLI to fetch and verify one final chunk. Finally it sends a graceful interrupt and verifies normal shutdown removed the peer and all ownership rows. The E2E uses a 2-second heartbeat, 8-second TTL, and 60-second cleanup interval; only its isolated test database is dropped.

This is process-level evidence for the checked-in Uploader lifecycle, not a mock. It does not establish parallel or complete-file behavior for the Downloader. The separate `chunk_probe.py` remains a controlled test client, distinct from the actual Downloader CLI.

### Final regression results

| Suite | Result | Runtime reported by unittest |
|---|---:|---:|
| Shared heartbeat client | 7 passed, 0 failed | 1.265 s |
| PostgreSQL | 21 passed, 0 failed | 25.057 s |
| Tracker TCP | 12 passed, 0 failed | 2.697 s |
| New real-Uploader recovery E2E | 1 passed, 0 failed | 10.060 s |
| Both real-process E2E cases | 2 passed, 0 failed | 22.337 s |
| Full suite | 75 passed, 0 failed, 0 skipped | 60.909 s |

The PostgreSQL suite includes the real TCP/DB heartbeat recovery callback coverage, TTL filtering, cleanup cascade, rollback, concurrent queries, and the heartbeat/cleanup race. The TCP suite includes concurrent clients, malformed and incomplete frames, timeout, and shutdown behavior.

### T01 and protocol alignment

`docs/PROTOCOL.md` records the current framing/RPC/metadata/error contract and identifies the UUID validation, implementation-only `get_peers.peer_id`, `RATE_LIMITED`, no-wire-version, and incomplete-frame differences between the group document and implementation. T01 remains **PARTIAL** pending the group's decision on whether an incomplete frame gets no response or an `INVALID_REQUEST` response with a null request ID. The current code closes an incomplete frame without a response and logs the endpoint plus timeout/EOF cause. This work documents the recommendation but intentionally does not change the Tracker or the test plan.

Group rule 05 says the repository protocol copy and `04_Giao_thuc_TCP_JSON.docx` must stay synchronized, and changes require a reviewed issue/PR. The DOCX has no numeric semantic protocol version or wire negotiation field. This branch creates the repository copy and flags implementation mismatches; it does not claim the group DOCX/Drive copy has been synchronized or that the group has selected a numeric versioning scheme.

### Scope boundary

- Student 2's currently checked-in Uploader is still sequential and has no bounded worker pool or upload throttling. `RATE_LIMITED` is listed by the group protocol but is not emitted by this implementation.
- Student 3's checked-in Downloader downloads one chunk per invocation. It can try other peers from the initial Tracker response sequentially for that chunk, but has no parallel chunk scheduler, full-file assembly, Tracker refresh after source failure, or complete mid-transfer failover.
- The real Downloader E2E assertion covers one chunk; the controlled probe is the component that checks all chunks/full-file hash in the restart scenario. These are separate clients and evidence.
- The final group acceptance demo still needs two live Seeders, parallel multi-chunk download, assembled-file SHA-256, and mid-transfer Seeder failure/fallback.

## Pre-PR review update — 9 November 2026 (before T01 follow-up)

The pre-PR review found one concrete shutdown edge case in `PeerHeartbeat.stop()`: the default join budget allowed roughly one socket timeout, although an in-flight RPC can spend time connecting and then waiting for its response. The default budget is now `max(interval_seconds, 2 * timeout_seconds) + 1`, while callers can still provide `join_timeout`. A deterministic unit regression test covers delayed connect followed by delayed response. No Tracker API, Student 2/3 implementation, or T01 behavior changed.

After this fix, the heartbeat client suite passed 8/8 in 4.497 seconds and the full suite passed **76/76, 0 skipped in 73.634 seconds**. The earlier 75/75, 74/74, and 70/70 full-suite runs above remain historical milestones. Both real-process E2Es passed again in the final full-suite run.

The review also marked `get_peers.peer_id` as an implementation-only extension in `docs/PROTOCOL.md`: the Tracker supports it, but the group protocol document does not list it. It must not be presented as a shared contract until the group approves and synchronizes the protocol copies. At that review point T01 remained unresolved; the follow-up below supersedes that status.

## T01 completion follow-up — 9 November 2026

T01 is now **PASS** against the test plan and the group protocol's null request ID rule. The Tracker rejects an incomplete prefix/body without processing it, logs `error_code=INVALID_REQUEST` with the remote endpoint and cause type, attempts exactly one short error response with `request_id: null`, then closes the connection. If the peer has disconnected or the socket cannot send, the send failure is logged and absorbed without retrying or stopping the server. Complete malformed frames and decoded requests with missing/invalid IDs also receive `INVALID_REQUEST` with a null ID.

Five T01 tests cover a short prefix followed by EOF, a valid length prefix with a payload timeout, malformed JSON, a missing request ID, and client disconnect while the Tracker is reading. They verify response shape when writable, connection closure, error logging, no sensitive response text, and continued service to a later valid client. The shared field names and RPC contract were not changed; the ambiguity was resolved by making the null-ID error reply conditional on the socket still being writable. The source DOCX files were left unchanged.

Final regression results after T01:

| Suite | Result | Runtime reported by unittest |
|---|---:|---:|
| T01-focused TCP cases | 5 passed, 0 failed | 1.008 s |
| Tracker TCP | 16 passed, 0 failed | 3.168 s |
| Heartbeat client | 8 passed, 0 failed | 4.474 s |
| PostgreSQL | 21 passed, 0 failed | 27.732 s |
| Both real-process E2Es | 2 passed, 0 failed | 25.714 s |
| Full suite | **80 passed, 0 failed, 0 skipped** | **64.000 s** |

The previous 76/76 result remains a historical pre-T01 milestone. The README, protocol, demo guide, and this handover now identify 80/80 as the latest run. The E2E scope remains one verified Downloader chunk; it does not prove parallel download, full-file assembly, or full-file failover.
