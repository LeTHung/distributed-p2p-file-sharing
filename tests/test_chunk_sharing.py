import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from common.framing import ProtocolError
from common.metadata import build_metadata
from peer.chunk_sharing import ChunkSharing, ChunkStore, _OwnershipHeartbeat
from peer.downloader import DownloadError, fetch_chunk_from_peer


class ChunkSharingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        source = self.root / "original.bin"
        source.write_bytes(b"abcdefghij")
        self.metadata = build_metadata(source, 4)
        self.parts = self.root / "parts"
        self.parts.mkdir()

    def test_store_only_serves_verified_parts_and_completed_file(self):
        store = ChunkStore(self.metadata, self.parts)
        self.assertEqual(store.available_indexes(), [])
        (self.parts / "0.part").write_bytes(b"abcd")
        (self.parts / "1.part").write_bytes(b"bad")
        self.assertEqual(store.available_indexes(), [0])
        with self.assertRaises(OSError):
            store.read_chunk(1)
        (self.parts / "1.part").unlink()
        (self.parts / "0.part").unlink()
        (self.parts / "completed").mkdir()
        (self.parts / "completed" / "original.bin").write_bytes(b"abcdefghij")
        self.assertEqual(store.available_indexes(), [0, 1, 2])
        self.assertEqual(store.read_chunk(2), b"ij")

    def test_announced_endpoint_serves_part_and_unregisters_when_closed(self):
        requests = []
        with patch('peer.chunk_sharing.tracker_request', side_effect=lambda a,p,t: requests.append(p) or {"ok": True}):
            with ChunkSharing(("127.0.0.1", 9000), self.metadata, self.parts, "partial-peer", interval=60) as sharing:
                endpoint = (requests[0]["host"], requests[0]["port"])
                self.assertGreater(endpoint[1], 0)
                self.assertEqual(requests[2]["chunk_indexes"], [])
                (self.parts / "0.part").write_bytes(b"abcd")
                sharing.add_chunk(0)
                sharing.publish_current()
                self.assertEqual(requests[-1]["chunk_indexes"], [0])
                self.assertEqual(fetch_chunk_from_peer(endpoint, self.metadata["file_id"], 0), b"abcd")
                with self.assertRaises(DownloadError):
                    fetch_chunk_from_peer(endpoint, self.metadata["file_id"], 1)
            self.assertFalse(sharing.thread.is_alive())
            self.assertEqual(requests[-1]["type"], "unregister_peer")

    def test_failed_announce_retries_after_next_successful_heartbeat(self):
        owner = Mock()
        owner.publish_current.side_effect = [ProtocolError("temporary network error"), None]
        heartbeat = _OwnershipHeartbeat(("127.0.0.1", 9000), "partial", owner=owner,
                                        interval_seconds=1, timeout_seconds=1)
        with patch('common.heartbeat.PeerHeartbeat.send_once', return_value={"ok": True}):
            heartbeat.send_once()
            heartbeat.send_once()
        self.assertEqual(owner.publish_current.call_count, 2)

    def test_retry_refresh_can_find_source_missing_in_initial_directory(self):
        from peer.downloader import _download_file_chunk
        fresh = {"peers": [{"peer_id": "new-source", "host": "127.0.0.1", "port": 7002,
                             "chunk_indexes": [0]}]}
        with patch('peer.downloader.time.sleep'), patch('peer.downloader.query_tracker', return_value=fresh), \
             patch('peer.downloader.fetch_chunk_from_peer', return_value=b"abcd"):
            result = _download_file_chunk(0, self.metadata["file_id"], self.metadata,
                          {c["chunk_index"]: c for c in self.metadata["chunks"]}, {}, self.parts,
                          tracker_address=("127.0.0.1", 9000))
        self.assertEqual(result, 0)
        self.assertEqual((self.parts / "0.part").read_bytes(), b"abcd")
