# TCP/JSON Protocol: Current Group Contract

This file records the current group contract in `04_Giao_thuc_TCP_JSON.docx` alongside the behavior implemented by the Tracker, Uploader, and Downloader in this repository. Differences are called out below and must not be treated as agreed API additions. The group rule in `05_Quy_tac_lam_viec_nhom.docx` requires both copies to stay aligned; protocol changes need a reviewed issue/PR and coordinated updates to dependent modules.

The group documents do not define a numeric semantic version, and the current wire format carries no `protocol_version` field or negotiation handshake. This document therefore identifies the contract by its source document and supplement date rather than introducing a version or wire field. A numeric versioning scheme must be agreed by the group before one is added.

## Transport and framing

- Control messages use TCP and UTF-8 JSON objects.
- Each JSON object is prefixed by an unsigned 4-byte big-endian byte length. The length counts the JSON bytes and excludes the prefix.
- Control JSON is limited to 1,048,576 bytes. The Tracker and peer programs process one request and one response per connection, then close it.
- A successful peer `get_chunk` response is a framed JSON header followed immediately by exactly `data_length` raw binary bytes. An error response has no binary body.
- The default socket deadline is 5 seconds (`SOCKET_TIMEOUT_SECONDS`). The receive helper uses a deadline for the complete frame/body rather than restarting the timeout for each partial read.
- The Tracker defaults to at most 64 concurrent client handlers (`TRACKER_MAX_CLIENTS`). It closes excess connections; it does not queue them.

Every request contains `type` and `request_id`. The group document asks clients to use a UUID; checked-in clients generate UUIDs, while Tracker validation currently accepts any nonempty string up to 256 characters. This is a documented contract/implementation difference: clients should send UUIDs; the Tracker does not enforce UUID syntax. Responses echo the request ID when one can be read. A response has `ok: true` on success. An application error has `ok: false`, `error_code`, and a safe `message`.

The Tracker ignores unrecognized extra request properties today. Clients must not rely on that permissiveness; send only fields listed below. JSON property names and chunk numbering are shared contract and must not be changed by one module alone.

## Tracker requests

All rows also require `type` and `request_id`.

| `type` | Required properties | Optional properties | Successful response data |
|---|---|---|---|
| `register_peer` | `peer_id` (nonempty string, at most 256 chars), `host` (unicast IPv4/IPv6 literal), `port` (integer 1–65535) | — | `peer_id` |
| `register_file` | `file_id`, `file_name`, `file_size`, `chunk_size`, `total_chunks`, `chunks` | — | `file_id` |
| `announce_chunks` | `peer_id`, `file_id`, `chunk_indexes` (array of nonnegative integer indexes) | — | `peer_id`, `file_id` |
| `get_peers` | `file_id` | `chunk_indexes` (array; omission means all chunks, `[]` means no requested chunks), `peer_id` (implementation-only extension; exclude this peer) | complete file metadata and `peers` |
| `heartbeat` | `peer_id` | — | `peer_id` |
| `unregister_peer` | `peer_id` | — | `peer_id` |
| `list_files` | — | — | `files` |

`peer_id` and `file_id` are nonempty strings (the latter must be lowercase 64-character SHA-256 hex). `chunk_indexes` values must be JSON integers, not booleans; duplicates are normalized. An index outside registered file metadata is `INVALID_REQUEST`.

### Known implementation/contract differences

- The checked-in Tracker accepts optional `get_peers.peer_id` to exclude a peer from results, but the group protocol document lists only optional `chunk_indexes`. Treat this as an implementation extension, not a shared API promise, until the group approves and synchronizes it.
- The group document requires UUID request IDs. Current Tracker validation checks only that `request_id` is a nonempty string of at most 256 characters; checked-in clients generate UUIDs.
- The group error list includes `RATE_LIMITED`; the checked-in Uploader has no upload throttling and does not emit that code.
- The source documents do not define a numeric semantic protocol version or a wire negotiation field.

### File and chunk metadata

`register_file` carries:

```json
{
  "file_id": "<lowercase SHA-256 of the complete file>",
  "file_name": "display-name.bin",
  "file_size": 2049,
  "chunk_size": 1024,
  "total_chunks": 3,
  "chunks": [
    {"chunk_index": 0, "chunk_hash": "<lowercase SHA-256 of chunk 0>"},
    {"chunk_index": 1, "chunk_hash": "<lowercase SHA-256 of chunk 1>"},
    {"chunk_index": 2, "chunk_hash": "<lowercase SHA-256 of chunk 2>"}
  ]
}
```

`total_chunks` equals `ceil(file_size / chunk_size)`. Chunk indexes start at zero, are contiguous and sorted in registration metadata, and each hash is lowercase SHA-256 hex. The final chunk may be shorter than `chunk_size`. The current Tracker rejects empty files. `file_name` is a display name only: it cannot contain a directory path or drive. A repeated `file_id` is accepted only when the saved size, chunk layout, and hashes match; persisted metadata is not replaced by a conflicting registration.

`get_peers` returns `file_id`, full metadata under `file`, and the same metadata fields at the top level for existing client compatibility. `peers` contains `peer_id`, `host`, `port`, and the matching owned `chunk_indexes`. A known file with no eligible source returns success with `peers: []`; an unknown file returns `FILE_NOT_FOUND`. `list_files` returns each file once if it has at least one active, nonexpired owner.

### Ownership and peer lifecycle

`register_peer` records the peer endpoint and marks it active. `register_file` stores file/chunk metadata but does not create ownership. `announce_chunks` adds ownership rows; it is additive and idempotent, so repeating it does not remove previously announced chunks. It requires an active, nonexpired registered peer.

The current defaults are a 5-second heartbeat (`HEARTBEAT_INTERVAL_SECONDS`), 20-second peer TTL (`PEER_TTL_SECONDS`), and 5-second background cleanup interval (`TRACKER_CLEANUP_INTERVAL_SECONDS`). The TTL must be greater than the heartbeat interval. `get_peers` and `list_files` filter expired peers immediately. Cleanup deletes expired peers and cascades their ownership rows; file and chunk metadata remain. A heartbeat for an expired or unknown peer returns `PEER_NOT_FOUND`. The returning peer must register again and announce all chunks it currently owns; old ownership is intentionally cleared on re-registration after expiry.

Normal `unregister_peer` removes the peer and its ownership immediately. Calling it for an unknown peer returns `PEER_NOT_FOUND`. Hard process loss is handled by TTL cleanup.

The shared `common.heartbeat.PeerHeartbeat` sends heartbeats, classifies Tracker/transport errors, and invokes its callback; it does not re-register or know file ownership. The checked-in Uploader's `SeederRegistration` handles an RPC failure by retrying registration, file metadata, and all chunk announcements on its next lifecycle tick.

## Peer-to-peer `get_chunk`

The request contains `type: "get_chunk"`, `request_id`, `file_id`, and integer `chunk_index`. On success, the Uploader sends a framed JSON header containing `request_id`, `ok: true`, `file_id`, `chunk_index`, and `data_length`, then sends exactly that many bytes. The Downloader checks the request ID, file/index, announced length, expected length from metadata, and SHA-256 before saving a `.part` file.

The current Uploader response errors include `INVALID_REQUEST`, `FILE_NOT_FOUND`, `CHUNK_NOT_FOUND`, and `CHUNK_UNAVAILABLE`. The Tracker emits `INVALID_REQUEST`, `PEER_NOT_FOUND`, `FILE_NOT_FOUND`, and `INTERNAL_ERROR` for its implemented paths. The group protocol also lists `RATE_LIMITED`, but the checked-in Uploader has no upload throttling and does not emit that code. Do not treat it as implemented until Student 2 integrates and tests the throttling change.

## Timeouts, malformed frames, and connection failures

The current client and server timeout defaults are 5 seconds. A complete but invalid frame is logged as `INVALID_REQUEST` and receives an error response with `request_id: null`. An incomplete prefix/body that reaches EOF or its receive deadline is also logged as `INVALID_REQUEST`, with the remote endpoint and exception type. The Tracker attempts one null-ID error response, then closes; if the client has disconnected, the failed send is logged and is not retried. Database and unexpected internal failures return `INTERNAL_ERROR`; sensitive database details are not sent to clients.

### T01: incomplete or invalid request frame

The Tracker classifies incomplete prefixes/bodies, malformed complete JSON, invalid frame lengths, and parsed requests without a usable `request_id` as `INVALID_REQUEST`. For incomplete input it includes the remote endpoint and cause type in the log. It makes one attempt to send an error response with `request_id: null`, `ok: false`, `error_code: "INVALID_REQUEST"`, and a short generic message, then closes the connection. If the client has disconnected or the socket cannot send, the send failure is logged and absorbed; the server does not retry the response. A malformed complete JSON frame uses the same error structure. A parsed object with a missing or invalid request ID also receives `request_id: null`.

This resolves T01 by combining the test plan's required safe close and `INVALID_REQUEST` log with the protocol document's null request ID rule. No request fields or RPC meanings were changed. Tests cover a short prefix followed by EOF, a valid prefix with a payload timeout, malformed JSON, missing request ID, client disconnect during frame reading, and a subsequent valid request.

## Compatibility and scope

There is no negotiated wire version. For compatible evolution, keep existing names, types, required fields, framing, hash rules, and response meanings unchanged. Any incompatible change needs a coordinated group decision, updates to `04_Giao_thuc_TCP_JSON.docx` and this file in the same reviewed PR, and corresponding changes/tests in Tracker, Uploader, and Downloader. Do not infer new fields from this document.

Current Downloader code requests one chunk per CLI invocation. It tries the initial Tracker result's peers sequentially for that chunk, but does not schedule multiple chunks in parallel, re-query after the candidate list fails, reassemble a complete file, or provide the planned full transfer failover. The current Uploader serves requests sequentially and does not have the planned bounded worker pool or upload throttling. These are implementation gaps, not protocol guarantees.
