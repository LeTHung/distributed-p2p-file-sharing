"""Week-one Leecher / Downloader: Tải và xác thực 1 chunk từ Seeder."""

from __future__ import annotations

import argparse
import hashlib
import logging
from pathlib import Path
import socket
import time
from typing import Any
import uuid

from common.config import load_settings
from common.framing import ProtocolError, recv_exact, recv_json, send_json
from common.metadata import HASH, validate_metadata

LOG = logging.getLogger(__name__)


class DownloadError(Exception):
    """Exception đại diện cho các lỗi khi kết nối hoặc xác thực chunk."""
    pass


def query_tracker(
    tracker_address: tuple[str, int],
    file_id: str,
    chunk_indexes: list[int] | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """1. Gửi RPC 'get_peers' lên Tracker để lấy metadata file và danh sách Seeder."""
    request_id = str(uuid.uuid4())
    payload: dict[str, Any] = {
        "type": "get_peers",
        "request_id": request_id,
        "file_id": file_id,
    }
    if chunk_indexes is not None:
        payload["chunk_indexes"] = chunk_indexes

    try:
        with socket.create_connection(tracker_address, timeout=timeout) as sock:
            send_json(sock, payload)
            response = recv_json(sock, timeout=timeout)
    except (OSError, EOFError, ProtocolError) as err:
        LOG.error("Lỗi socket khi kết nối tới Tracker %s: %s", tracker_address, err)
        raise DownloadError(f"Không thể kết nối Tracker: {err}") from err

    # Kiểm tra tính hợp lệ của response từ Tracker
    if response.get("request_id") != request_id:
        raise DownloadError("Tracker trả về request_id không khớp với request_id gửi đi")

    if response.get("ok") is not True:
        error_code = response.get("error_code", "UNKNOWN")
        message = response.get("message", "Lỗi không xác định")
        raise DownloadError(f"Tracker từ chối [{error_code}]: {message}")

    # Current Tracker returns top-level metadata; the Drive contract nests it.
    # Normalize at the boundary so either deployment can serve the Leecher.
    metadata = response.get("file", response)
    try:
        if not isinstance(metadata, dict):
            raise ValueError("file must be an object")
        validate_metadata(metadata)
        if metadata["file_id"] != file_id:
            raise ValueError("file_id does not match request")
        peers = response.get("peers")
        if not isinstance(peers, list):
            raise ValueError("peers must be a list")
        for peer in peers:
            if (not isinstance(peer, dict) or not isinstance(peer.get("host"), str) or
                    not peer["host"] or type(peer.get("port")) is not int or
                    not 1 <= peer["port"] <= 65535 or
                    not isinstance(peer.get("chunk_indexes"), list) or
                    any(type(i) is not int or not 0 <= i < metadata["total_chunks"]
                        for i in peer["chunk_indexes"])):
                raise ValueError("invalid peer endpoint or chunk indexes")
    except ValueError as error:
        raise DownloadError(f"Metadata Tracker không hợp lệ: {error}") from error
    return {**response, "file": metadata}


def fetch_chunk_from_peer(
    peer_address: tuple[str, int],
    file_id: str,
    chunk_index: int,
    timeout: float = 5.0,
    *,
    expected_length: int | None = None,
) -> bytes:
    """2. Kết nối tới Seeder, gửi 'get_chunk', nhận JSON Header và đọc đúng số byte nhị phân."""
    request_id = str(uuid.uuid4())
    payload = {
        "type": "get_chunk",
        "request_id": request_id,
        "file_id": file_id,
        "chunk_index": chunk_index,
    }

    try:
        with socket.create_connection(peer_address, timeout=timeout) as sock:
            send_json(sock, payload)
            header = recv_json(sock, timeout=timeout)

            # Kiểm tra header phản hồi
            if header.get("request_id") != request_id:
                raise DownloadError("request_id từ Seeder không khớp")

            if header.get("ok") is not True:
                error_code = header.get("error_code", "UNKNOWN")
                msg = header.get("message", "")
                raise DownloadError(f"Seeder từ chối gửi chunk [{error_code}]: {msg}")

            if header.get("file_id") != file_id or header.get("chunk_index") != chunk_index:
                raise DownloadError("Thông tin file_id hoặc chunk_index từ Seeder không khớp")

            data_length = header.get("data_length")
            if type(data_length) is not int or data_length < 0:
                raise DownloadError(f"data_length không hợp lệ: {data_length}")
            if expected_length is not None and data_length != expected_length:
                raise DownloadError("data_length không khớp metadata")

            # Đọc chính xác data_length byte nhị phân theo sau JSON header
            chunk_bytes = recv_exact(sock, data_length, deadline=time.monotonic() + timeout)
            return chunk_bytes

    except (OSError, EOFError, ProtocolError) as err:
        LOG.warning("Lỗi khi tải chunk %d từ Seeder %s: %s", chunk_index, peer_address, err)
        raise DownloadError(f"Lỗi kết nối tới Seeder {peer_address}: {err}") from err


def verify_chunk_integrity(
    data: bytes,
    expected_hash: str,
    expected_length: int | None = None,
) -> bool:
    """3. Kiểm tra tính toàn vẹn: so khớp độ dài và mã băm SHA-256."""
    if expected_length is not None and len(data) != expected_length:
        LOG.error("Sai kích thước chunk: nhận %d byte, kỳ vọng %d byte", len(data), expected_length)
        return False

    actual_hash = hashlib.sha256(data).hexdigest()
    if actual_hash.lower() != expected_hash.lower():
        LOG.error("Sai mã SHA-256! Nhận: %s, Kỳ vọng: %s", actual_hash, expected_hash)
        return False

    return True


def save_chunk(data: bytes, destination_path: Path | str) -> Path:
    """4. Lưu dữ liệu nhị phân của chunk ra đĩa thành file .part."""
    path = Path(destination_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        f.write(data)
    return path


def download_single_chunk(
    file_id: str,
    chunk_index: int = 0,
    output_dir: Path | str = "downloads",
    tracker_address: tuple[str, int] | None = None,
    direct_peer_address: tuple[str, int] | None = None,
    expected_hash: str | None = None,
    timeout: float = 5.0,
) -> Path:
    """Luồng điều phối chính (Orchestrator):
    - Hỏi Tracker -> Lấy metadata & danh sách peer -> Tải chunk -> Xác thực SHA-256 -> Lưu .part
    - Hỗ trợ tải trực tiếp từ Seeder nếu truyền direct_peer_address (để test khi chưa có Tracker).
    """
    if not isinstance(file_id, str) or not HASH.fullmatch(file_id):
        raise DownloadError("file_id phải là SHA-256 hex chữ thường, đủ 64 ký tự")
    if type(chunk_index) is not int or chunk_index < 0:
        raise DownloadError("chunk_index phải là số nguyên không âm")
    if timeout <= 0:
        raise DownloadError("timeout phải lớn hơn 0")
    dest_dir = Path(output_dir)
    target_part = dest_dir / file_id / f"{chunk_index}.part"

    # Nhánh A: Chạy độc lập trực tiếp với Seeder (Bypass Tracker)
    if direct_peer_address is not None:
        if not isinstance(expected_hash, str) or not HASH.fullmatch(expected_hash):
            raise DownloadError("Tải trực tiếp cần expected_hash SHA-256 hợp lệ để xác thực")
        LOG.info("Đang tải trực tiếp chunk %d từ Seeder %s...", chunk_index, direct_peer_address)
        chunk_data = fetch_chunk_from_peer(direct_peer_address, file_id, chunk_index, timeout=timeout)
        if not verify_chunk_integrity(chunk_data, expected_hash):
            raise DownloadError(f"Xác thực SHA-256 chunk {chunk_index} thất bại!")
        return save_chunk(chunk_data, target_part)

    # Nhánh B: Luồng chuẩn qua Tracker
    if tracker_address is None:
        raise ValueError("Cần cung cấp tracker_address")

    # B1: Hỏi Tracker
    tracker_res = query_tracker(tracker_address, file_id, chunk_indexes=[chunk_index], timeout=timeout)
    file_meta = tracker_res["file"]

    # B2: Lấy thông tin chunk từ metadata
    chunk_meta = next((c for c in file_meta["chunks"] if c.get("chunk_index") == chunk_index), None)
    if not chunk_meta:
        raise DownloadError(f"Không tìm thấy chunk index {chunk_index} trong metadata của file")

    exp_hash = chunk_meta["chunk_hash"]
    # Tính kích thước chuẩn của chunk (chunk cuối có thể ngắn hơn chunk_size)
    exp_len = min(file_meta["chunk_size"], file_meta["file_size"] - chunk_index * file_meta["chunk_size"])

    # B3: Tìm peer online có sở hữu chunk này
    available_peers = [
        (p["host"], p["port"], p.get("peer_id", "peer"))
        for p in tracker_res.get("peers", [])
        if chunk_index in p.get("chunk_indexes", [])
    ]
    if not available_peers:
        raise DownloadError(f"Tracker báo không có peer nào đang online giữ chunk {chunk_index}")

    # B4: Tải và thử lần lượt các peer (failover đơn giản)
    for host, port, peer_id in available_peers:
        try:
            LOG.info("Tải chunk %d từ %s (%s:%d)...", chunk_index, peer_id, host, port)
            chunk_data = fetch_chunk_from_peer((host, port), file_id, chunk_index,
                                               timeout=timeout, expected_length=exp_len)

            # Xác thực
            if not verify_chunk_integrity(chunk_data, exp_hash, exp_len):
                LOG.warning("Chunk từ peer %s sai SHA-256, bỏ qua và thử peer khác...", peer_id)
                continue

            saved_path = save_chunk(chunk_data, target_part)
            LOG.info("Chunk %d đã được lưu thành công: %s", chunk_index, saved_path)
            return saved_path
        except DownloadError as err:
            LOG.warning("Peer %s gặp lỗi: %s, chuyển sang peer khác...", peer_id, err)
            continue

    raise DownloadError(f"Tất cả các Seeder cho chunk {chunk_index} đều thất bại.")


def main() -> None:
    settings = load_settings()

    parser = argparse.ArgumentParser(description="Week-one Leecher: Download and verify one chunk from Seeder")
    parser.add_argument("file_id", help="SHA-256 hex  of the file to download")
    parser.add_argument("--chunk-index", type=int, default=0, help="Index of the chunk to download (default: 0)")
    parser.add_argument("--output-dir", type=Path, default=Path("downloads"), help="Directory to save .part file")
    parser.add_argument("--tracker-host", default=settings.tracker_host, help="Tracker host address")
    parser.add_argument("--tracker-port", type=int, default=settings.tracker_port, help="Tracker port")
    parser.add_argument("--peer-host", help="Direct Seeder host (bypass Tracker for testing)")
    parser.add_argument("--peer-port", type=int, help="Direct Seeder port")
    parser.add_argument("--expected-hash", help="Expected SHA-256 hex when downloading directly")

    args = parser.parse_args()
    if bool(args.peer_host) != (args.peer_port is not None):
        parser.error("--peer-host và --peer-port phải đi cùng nhau")
    if args.peer_port is not None and not 1 <= args.peer_port <= 65535:
        parser.error("--peer-port phải từ 1 đến 65535")
    if not 1 <= args.tracker_port <= 65535:
        parser.error("--tracker-port phải từ 1 đến 65535")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    direct_addr = (args.peer_host, args.peer_port) if args.peer_host and args.peer_port else None
    tracker_addr = (args.tracker_host, args.tracker_port) if direct_addr is None else None

    try:
        saved_file = download_single_chunk(
            file_id=args.file_id,
            chunk_index=args.chunk_index,
            output_dir=args.output_dir,
            tracker_address=tracker_addr,
            direct_peer_address=direct_addr,
            expected_hash=args.expected_hash,
        )
        print(f"\n[HOÀN THÀNH] Đã tải và xác thực thành công chunk {args.chunk_index}: {saved_file}")
    except Exception as exc:
        LOG.error("Quá trình tải thất bại: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
