-- PostgreSQL schema for the Tracker only.

CREATE TABLE peers (
    peer_id TEXT PRIMARY KEY,
    host INET NOT NULL,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
);

CREATE TABLE files (
    file_id CHAR(64) PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_size BIGINT NOT NULL CHECK (file_size >= 0),
    chunk_size INTEGER NOT NULL CHECK (chunk_size > 0),
    total_chunks INTEGER NOT NULL CHECK (total_chunks > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chunks (
    file_id CHAR(64) NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    chunk_hash CHAR(64) NOT NULL,
    PRIMARY KEY (file_id, chunk_index)
);

CREATE TABLE peer_chunks (
    peer_id TEXT NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    file_id CHAR(64) NOT NULL,
    chunk_index INTEGER NOT NULL,
    announced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (peer_id, file_id, chunk_index),
    FOREIGN KEY (file_id, chunk_index)
        REFERENCES chunks(file_id, chunk_index) ON DELETE CASCADE
);

CREATE INDEX peer_chunks_by_file_chunk ON peer_chunks (file_id, chunk_index);
CREATE INDEX peers_last_seen_active ON peers (last_seen) WHERE status = 'active';
