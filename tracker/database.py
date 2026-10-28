"""PostgreSQL directory. Each operation owns one connection and transaction."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from common.config import Settings
from tracker.errors import RequestError


class Directory:
    def __init__(self, settings: Settings, connect: Callable | None = None):
        self.ttl = settings.peer_ttl_seconds
        self._connect = connect or (lambda: psycopg.connect(
            host=settings.postgres_host, port=settings.postgres_port,
            dbname=settings.postgres_db, user=settings.postgres_user,
            password=settings.postgres_password, connect_timeout=5,
            options="-c statement_timeout=5000 -c lock_timeout=3000",
            row_factory=dict_row))

    def initialize(self) -> None:
        schema = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
        with self._connect() as conn:
            # Serialize concurrent initializers without modifying unrelated application data.
            conn.execute("SELECT pg_advisory_xact_lock(1405, 1)")
            conn.execute(schema.read_text(encoding="utf-8"))

    def prune_expired(self) -> int:
        with self._connect() as conn:
            # DELETE rechecks its predicate after waiting for a concurrent heartbeat's row lock.
            return conn.execute("DELETE FROM peers WHERE last_seen <= clock_timestamp() - %s * INTERVAL '1 second'",
                                (self.ttl,)).rowcount

    @staticmethod
    def _file(conn, file_id: str) -> dict[str, Any]:
        row = conn.execute("SELECT file_id, file_name, file_size, chunk_size, total_chunks FROM files WHERE file_id=%s",
                           (file_id,)).fetchone()
        if row is None:
            raise RequestError("FILE_NOT_FOUND", "File is not registered")
        return row

    @staticmethod
    def _indexes(request, metadata):
        indexes = request.get("chunk_indexes")
        if indexes and indexes[-1] >= metadata["total_chunks"]:
            raise RequestError("CHUNK_NOT_FOUND", "Chunk index is outside the file")
        return indexes

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        with self._connect() as conn:
            return getattr(self, "_" + request["type"])(conn, request)

    def _register_peer(self, conn, r):
        # A returning peer must not revive pre-expiry ownership accidentally.
        # Registration/heartbeat/announce/pruning serialize on the peer row.
        conn.execute("""INSERT INTO peers (peer_id,host,port,last_seen) VALUES (%s,%s,%s,clock_timestamp())
                     ON CONFLICT (peer_id) DO UPDATE SET peer_id=EXCLUDED.peer_id""",
                     (r["peer_id"], r["host"], r["port"]))
        previous = conn.execute("""SELECT host(host) AS host, port,
                               (status='active' AND last_seen > clock_timestamp() - %s * INTERVAL '1 second') AS live
                               FROM peers WHERE peer_id=%s FOR UPDATE""", (self.ttl, r["peer_id"])).fetchone()
        if previous and (not previous["live"] or previous["host"] != r["host"] or previous["port"] != r["port"]):
            conn.execute("DELETE FROM peer_chunks WHERE peer_id=%s", (r["peer_id"],))
        conn.execute("""UPDATE peers SET host=%s, port=%s, last_seen=clock_timestamp(), status='active'
                     WHERE peer_id=%s""", (r["host"], r["port"], r["peer_id"]))
        return {"peer_id": r["peer_id"]}

    def _register_file(self, conn, r):
        conn.execute("""INSERT INTO files (file_id,file_name,file_size,chunk_size,total_chunks)
                     VALUES (%s,%s,%s,%s,%s) ON CONFLICT (file_id) DO NOTHING""",
                     tuple(r[k] for k in ("file_id", "file_name", "file_size", "chunk_size", "total_chunks")))
        stored = self._file(conn, r["file_id"])
        if any(stored[k] != r[k] for k in ("file_size", "chunk_size", "total_chunks")):
            raise RequestError("INVALID_REQUEST", "File ID already has different metadata")
        expected = sorted(r["chunks"], key=lambda c: c["chunk_index"])
        # The INSERT above waits on a concurrent registration of the same file ID.
        with conn.cursor() as cursor:
            cursor.executemany("""INSERT INTO chunks (file_id,chunk_index,chunk_hash) VALUES (%s,%s,%s)
                               ON CONFLICT (file_id,chunk_index) DO NOTHING""",
                               [(r["file_id"], c["chunk_index"], c["chunk_hash"]) for c in expected])
        chunks = conn.execute("SELECT chunk_index, chunk_hash FROM chunks WHERE file_id=%s ORDER BY chunk_index",
                              (r["file_id"],)).fetchall()
        if chunks != [{"chunk_index": c["chunk_index"], "chunk_hash": c["chunk_hash"]} for c in expected]:
            raise RequestError("INVALID_REQUEST", "File ID already has different chunk hashes")
        return {"file_id": r["file_id"]}

    def _announce_chunks(self, conn, r):
        peer = conn.execute("""SELECT peer_id FROM peers WHERE peer_id=%s AND status='active'
                             AND last_seen > clock_timestamp() - %s * INTERVAL '1 second' FOR UPDATE""",
                             (r["peer_id"], self.ttl)).fetchone()
        if peer is None:
            raise RequestError("PEER_NOT_FOUND", "Peer must register before announcing chunks")
        self._indexes(r, self._file(conn, r["file_id"]))
        with conn.cursor() as cursor:
            cursor.executemany("""INSERT INTO peer_chunks (peer_id,file_id,chunk_index) VALUES (%s,%s,%s)
                               ON CONFLICT (peer_id,file_id,chunk_index) DO NOTHING""",
                               [(r["peer_id"], r["file_id"], i) for i in r["chunk_indexes"]])
        return {"peer_id": r["peer_id"], "file_id": r["file_id"]}

    def _get_peers(self, conn, r):
        metadata = self._file(conn, r["file_id"])
        indexes = self._indexes(r, metadata)
        rows = conn.execute("""SELECT p.peer_id, host(p.host) AS host, p.port,
                           array_agg(pc.chunk_index ORDER BY pc.chunk_index) AS chunk_indexes
                           FROM peers p JOIN peer_chunks pc USING (peer_id)
                           WHERE pc.file_id=%s AND p.status='active'
                             AND p.last_seen > clock_timestamp() - %s * INTERVAL '1 second'
                             AND (%s::text IS NULL OR p.peer_id<>%s)
                             AND (%s::integer[] IS NULL OR pc.chunk_index=ANY(%s::integer[]))
                           GROUP BY p.peer_id ORDER BY p.peer_id""",
                           (r["file_id"], self.ttl, r.get("peer_id"), r.get("peer_id"), indexes, indexes)).fetchall()
        # Additional metadata fields let a Leecher verify hashes without direct DB access.
        metadata["chunks"] = conn.execute("SELECT chunk_index,chunk_hash FROM chunks WHERE file_id=%s ORDER BY chunk_index",
                                          (r["file_id"],)).fetchall()
        return {**metadata, "peers": rows}

    def _heartbeat(self, conn, r):
        row = conn.execute("""UPDATE peers SET last_seen=clock_timestamp() WHERE peer_id=%s
                           AND status='active' AND last_seen > clock_timestamp() - %s * INTERVAL '1 second'
                           RETURNING peer_id""", (r["peer_id"], self.ttl)).fetchone()
        if row is None:
            raise RequestError("PEER_NOT_FOUND", "Peer expired or is not registered; register and announce again")
        return row

    def _unregister_peer(self, conn, r):
        row = conn.execute("DELETE FROM peers WHERE peer_id=%s RETURNING peer_id", (r["peer_id"],)).fetchone()
        if row is None:
            raise RequestError("PEER_NOT_FOUND", "Peer is not registered")
        return row

    def _list_files(self, conn, r):
        rows = conn.execute("""SELECT f.file_id,f.file_name,f.file_size,f.chunk_size,f.total_chunks FROM files f
                           WHERE EXISTS (SELECT 1 FROM peer_chunks pc JOIN peers p USING (peer_id)
                             WHERE pc.file_id=f.file_id AND p.status='active'
                             AND p.last_seen > clock_timestamp() - %s * INTERVAL '1 second')
                           ORDER BY f.file_name,f.file_id""", (self.ttl,)).fetchall()
        return {"files": rows}
