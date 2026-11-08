"""Unit tests cho module Leecher / Downloader (Tuần 1)."""

from __future__ import annotations

import hashlib
from pathlib import Path
import socketserver
import tempfile
import threading
from typing import cast
import unittest
from unittest.mock import patch

from common.framing import recv_json, send_json
from peer.downloader import (
    DownloadError,
    download_single_chunk,
    fetch_chunk_from_peer,
    save_chunk,
    verify_chunk_integrity,
    query_tracker,
)
from peer.uploader import SharedFile, UploaderServer


def get_server_addr(server: socketserver.TCPServer) -> tuple[str, int]:
    """Helper chuyển đổi server_address sang đúng kiểu tuple[str, int] để Pylance không bắt lỗi."""
    return str(server.server_address[0]), int(server.server_address[1])


class MockTrackerHandler(socketserver.BaseRequestHandler):
    """Giả lập Tracker TCP Server để test RPC 'get_peers'."""
    def handle(self):
        server = cast(MockTrackerServer, self.server)
        req = recv_json(self.request)
        req_id = req.get("request_id")
        file_id = req.get("file_id")

        if file_id == "unknown_file":
            send_json(self.request, {
                "request_id": req_id,
                "ok": False,
                "error_code": "FILE_NOT_FOUND",
                "message": "File not found on tracker",
            })
            return

        send_json(self.request, {
            "request_id": req_id,
            "ok": True,
            "file_id": file_id,
            "file": server.file_metadata,
            "peers": server.peer_list,
        })


class MockTrackerServer(socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, address, file_metadata: dict, peer_list: list):
        self.file_metadata = file_metadata
        self.peer_list = peer_list
        super().__init__(address, MockTrackerHandler)


class DownloaderTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.sample_path = Path(self.temp_dir.name) / "sample.bin"

        self.sample_data = b"0123456789abcdefghij!@#$%"
        self.sample_path.write_bytes(self.sample_data)

        self.shared_file = SharedFile(self.sample_path, chunk_size=10)
        self.seeder_server = UploaderServer(("127.0.0.1", 0), self.shared_file, timeout_seconds=2)
        self.seeder_thread = threading.Thread(target=self.seeder_server.serve_forever, daemon=True)
        self.seeder_thread.start()
        self.addCleanup(self._stop_seeder)

    def _stop_seeder(self):
        self.seeder_server.shutdown()
        self.seeder_server.server_close()
        self.seeder_thread.join(timeout=2)

    def test_verify_chunk_integrity(self):
        """Kiểm tra hàm xác thực hash và độ dài chunk."""
        data = b"test_chunk_data"
        correct_hash = hashlib.sha256(data).hexdigest()
        wrong_hash = hashlib.sha256(b"corrupted_data").hexdigest()

        self.assertTrue(verify_chunk_integrity(data, correct_hash, expected_length=len(data)))
        self.assertFalse(verify_chunk_integrity(data, wrong_hash, expected_length=len(data)))
        self.assertFalse(verify_chunk_integrity(data, correct_hash, expected_length=len(data) + 1))

    def test_save_chunk(self):
        """Kiểm tra việc lưu chunk ra file .part."""
        target_path = Path(self.temp_dir.name) / "downloads" / "sub" / "0.part"
        data = b"chunk_payload_123"
        saved_path = save_chunk(data, target_path)

        self.assertTrue(saved_path.exists())
        self.assertEqual(saved_path.read_bytes(), data)

    def test_fetch_chunk_from_peer_success_and_error(self):
        """Kiểm tra kết nối TCP tới Seeder để lấy chunk hợp lệ và chunk lỗi."""
        seeder_addr = get_server_addr(self.seeder_server)

        # Tải chunk 0 (10 bytes đầu)
        chunk_0 = fetch_chunk_from_peer(seeder_addr, self.shared_file.file_id, chunk_index=0)
        self.assertEqual(chunk_0, self.sample_data[:10])
        self.assertEqual(hashlib.sha256(chunk_0).hexdigest(), self.shared_file.chunks[0]["chunk_hash"])

        # Tải chunk 2 (chunk cuối ngắn hơn: 5 bytes)
        chunk_2 = fetch_chunk_from_peer(seeder_addr, self.shared_file.file_id, chunk_index=2)
        self.assertEqual(chunk_2, self.sample_data[20:])
        self.assertEqual(len(chunk_2), 5)

        # Yêu cầu chunk không tồn tại (index 99) -> phải ném DownloadError
        with self.assertRaises(DownloadError) as ctx:
            fetch_chunk_from_peer(seeder_addr, self.shared_file.file_id, chunk_index=99)
        self.assertIn("CHUNK_NOT_FOUND", str(ctx.exception))

    def test_download_single_chunk_direct(self):
        """Kiểm tra luồng tải trực tiếp (Bypass Tracker) với Seeder."""
        seeder_addr = get_server_addr(self.seeder_server)
        out_dir = Path(self.temp_dir.name) / "out_direct"

        saved_file = download_single_chunk(
            file_id=self.shared_file.file_id,
            chunk_index=1,
            output_dir=out_dir,
            direct_peer_address=seeder_addr,
            expected_hash=self.shared_file.chunks[1]["chunk_hash"],
        )

        self.assertTrue(saved_file.exists())
        self.assertEqual(saved_file.read_bytes(), self.sample_data[10:20])

    def test_rejects_unsafe_ids_and_unverified_direct_download(self):
        for file_id, index in [("../escape", 0), (self.shared_file.file_id, -1),
                               (self.shared_file.file_id, True)]:
            with self.subTest(file_id=file_id, index=index), self.assertRaises(DownloadError):
                download_single_chunk(file_id, index, direct_peer_address=get_server_addr(self.seeder_server))
        with self.assertRaisesRegex(DownloadError, "expected_hash"):
            download_single_chunk(self.shared_file.file_id,
                                  direct_peer_address=get_server_addr(self.seeder_server))

    def test_rejects_advertised_length_before_reading_body(self):
        with self.assertRaisesRegex(DownloadError, "data_length"):
            fetch_chunk_from_peer(get_server_addr(self.seeder_server), self.shared_file.file_id,
                                  0, expected_length=9)

    def test_tracker_top_level_metadata_is_compatible(self):
        # Use the real Tracker TCP handler with Directory's current response shape.
        from tracker.server import TrackerServer
        directory = unittest.mock.Mock()
        directory.execute.return_value = {**self.shared_file.metadata(), "peers": [{
            "peer_id": "seeder", "host": "127.0.0.1",
            "port": get_server_addr(self.seeder_server)[1], "chunk_indexes": [0, 1, 2]}]}
        with TrackerServer(("127.0.0.1", 0), directory) as tracker:
            thread = threading.Thread(target=tracker.serve_forever, daemon=True)
            thread.start()
            try:
                result = download_single_chunk(self.shared_file.file_id, 2,
                                               output_dir=Path(self.temp_dir.name) / "real-handler",
                                               tracker_address=get_server_addr(tracker))
                self.assertEqual(result.read_bytes(), self.sample_data[20:])
            finally:
                tracker.shutdown()
                thread.join(2)

    def test_invalid_tracker_metadata_is_rejected(self):
        invalid_metadata = [None, {**self.shared_file.metadata(), "chunks": []},
                            {**self.shared_file.metadata(), "file_id": "0" * 64}]
        for metadata in invalid_metadata:
            with self.subTest(metadata=metadata), patch("peer.downloader.recv_json") as receive:
                # Only network I/O is mocked; request correlation uses the real generated ID.
                def response(*args, **kwargs):
                    return {"request_id": sent[0]["request_id"], "ok": True,
                            "file": metadata, "peers": []}
                sent = []
                receive.side_effect = response
                with patch("peer.downloader.socket.create_connection"), \
                     patch("peer.downloader.send_json", side_effect=lambda sock, data: sent.append(data)):
                    with self.assertRaises(DownloadError):
                        query_tracker(("127.0.0.1", 9000), self.shared_file.file_id)

    def test_full_flow_with_mock_tracker(self):
        """Kiểm tra toàn bộ luồng tích hợp: Leecher -> Tracker -> Seeder -> Leecher."""
        seeder_addr = get_server_addr(self.seeder_server)

        peer_list = [{
            "peer_id": "seeder-hung",
            "host": seeder_addr[0],
            "port": seeder_addr[1],
            "chunk_indexes": [0, 1, 2],
        }]
        tracker_server = MockTrackerServer(("127.0.0.1", 0), self.shared_file.metadata(), peer_list)
        tracker_thread = threading.Thread(target=tracker_server.serve_forever, daemon=True)
        tracker_thread.start()

        def stop_tracker():
            tracker_server.shutdown()
            tracker_server.server_close()
            tracker_thread.join(timeout=2)
        self.addCleanup(stop_tracker)

        tracker_addr = get_server_addr(tracker_server)
        out_dir = Path(self.temp_dir.name) / "out_tracker"

        # Leecher tải chunk 0 qua Tracker
        saved_file = download_single_chunk(
            file_id=self.shared_file.file_id,
            chunk_index=0,
            output_dir=out_dir,
            tracker_address=tracker_addr,
        )

        self.assertTrue(saved_file.exists())
        self.assertEqual(saved_file.read_bytes(), self.sample_data[:10])

        # Leecher hỏi file không tồn tại trên Tracker -> Báo lỗi
        with self.assertRaises(DownloadError):
            download_single_chunk(
                file_id="unknown_file",
                chunk_index=0,
                output_dir=out_dir,
                tracker_address=tracker_addr,
            )


if __name__ == "__main__":
    unittest.main()
