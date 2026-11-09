"""Week-one Leecher / Downloader: Tải và xác thực 1 chunk từ Seeder."""

from __future__ import annotations

import argparse
import hashlib
import concurrent.futures
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
        LOG.exception("Lỗi socket khi kết nối tới Tracker %s: %s", tracker_address, err)
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


def _validate_single_chunk_request(file_id: str, chunk_index: int, timeout: float) -> None:
    if not isinstance(file_id, str) or not HASH.fullmatch(file_id):
        raise DownloadError("file_id phải là SHA-256 hex chữ thường, đủ 64 ký tự")
    if type(chunk_index) is not int or chunk_index < 0:
        raise DownloadError("chunk_index phải là số nguyên không âm")
    if timeout <= 0:
        raise DownloadError("timeout phải lớn hơn 0")


def _download_direct_chunk(
    file_id: str,
    chunk_index: int,
    target_part: Path,
    peer_address: tuple[str, int],
    expected_hash: str | None,
    timeout: float,
) -> Path:
    if not isinstance(expected_hash, str) or not HASH.fullmatch(expected_hash):
        raise DownloadError("Tải trực tiếp cần expected_hash SHA-256 hợp lệ để xác thực")
    LOG.info("Đang tải trực tiếp chunk %d từ Seeder %s...", chunk_index, peer_address)
    chunk_data = fetch_chunk_from_peer(peer_address, file_id, chunk_index, timeout=timeout)
    if not verify_chunk_integrity(chunk_data, expected_hash):
        raise DownloadError(f"Xác thực SHA-256 chunk {chunk_index} thất bại!")
    return save_chunk(chunk_data, target_part)


def _download_tracker_chunk(
    file_id: str,
    chunk_index: int,
    target_part: Path,
    tracker_address: tuple[str, int] | None,
    timeout: float,
) -> Path:
    if tracker_address is None:
        raise ValueError("Cần cung cấp tracker_address")

    tracker_res = query_tracker(tracker_address, file_id, chunk_indexes=[chunk_index], timeout=timeout)
    file_meta = tracker_res["file"]

    chunk_meta = next((c for c in file_meta["chunks"] if c.get("chunk_index") == chunk_index), None)
    if not chunk_meta:
        raise DownloadError(f"Không tìm thấy chunk index {chunk_index} trong metadata của file")

    exp_hash = chunk_meta["chunk_hash"]
    exp_len = min(file_meta["chunk_size"], file_meta["file_size"] - chunk_index * file_meta["chunk_size"])

    available_peers = [
        (p["host"], p["port"], p.get("peer_id", "peer"))
        for p in tracker_res.get("peers", [])
        if chunk_index in p.get("chunk_indexes", [])
    ]
    if not available_peers:
        raise DownloadError(f"Tracker báo không có peer nào đang online giữ chunk {chunk_index}")

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

def announce_chunk_to_tracker(
    tracker_address: tuple[str, int],
    peer_id: str,
    file_id: str,
    chunk_index: int,
    timeout: float = 5.0,
) -> bool:
    """Gửi RPC 'announce_chunks' lên Tracker để công bố Leecher đã sở hữu chunk này. 
    Đáp ứng yêu cầu Tuần 3: Announce chunk vừa tải để trở thành peer nguồn.
    """
    request_id = str(uuid.uuid4())
    payload = {
        "type": "announce_chunks",
        "request_id": request_id,
        "peer_id": peer_id,
        "file_id": file_id,
        "chunk_indexes": [chunk_index],
    }
    try:
        with socket.create_connection(tracker_address, timeout=timeout) as sock:
            send_json(sock, payload)
            response = recv_json(sock, timeout=timeout)
            if response.get("ok") is True:
                LOG.info("[ANNOUNCE] Đã công bố sở hữu chunk %d lên Tracker (peer_id=%s)", chunk_index, peer_id)
                return True
            else:
                LOG.warning("[ANNOUNCE] Tracker từ chối announce chunk %d: %s", chunk_index, response.get("message"))
                return False
    except (OSError, EOFError, ProtocolError) as err:
        LOG.warning("[ANNOUNCE] Không thể gửi announce chunk %d tới Tracker: %s", chunk_index, err)
        return False

def download_single_chunk(
    file_id: str,
    chunk_index: int = 0,
    output_dir: Path | str = "downloads",
    tracker_address: tuple[str, int] | None = None,
    direct_peer_address: tuple[str, int] | None = None,
    expected_hash: str | None = None,
    timeout: float = 5.0,
) -> Path:
    """Luồng điều phối chính: tải một chunk trực tiếp hoặc thông qua Tracker."""
    _validate_single_chunk_request(file_id, chunk_index, timeout)
    target_part = Path(output_dir) / file_id / f"{chunk_index}.part"

    if direct_peer_address is not None:
        return _download_direct_chunk(
            file_id, chunk_index, target_part, direct_peer_address, expected_hash, timeout
        )
    return _download_tracker_chunk(file_id, chunk_index, target_part, tracker_address, timeout)

def _validate_full_file_request(
    file_id: str,
    tracker_address: tuple[str, int] | None,
    max_workers: int,
) -> tuple[str, int]:
    if not isinstance(file_id, str) or not HASH.fullmatch(file_id):
        raise DownloadError("file_id phải là SHA-256 hex chữ thường, đủ 64 ký tự")
    if tracker_address is None:
        raise ValueError("Cần cung cấp tracker_address")
    if max_workers < 1:
        raise ValueError("max_workers phải >= 1")
    return tracker_address


def _get_full_file_response(
    file_id: str,
    tracker_address: tuple[str, int],
    timeout: float,
) -> dict[str, Any]:
    LOG.info("Hỏi Tracker %s metadata cho file %s...", tracker_address, file_id)
    tracker_res = query_tracker(tracker_address, file_id, timeout=timeout)
    file_meta = tracker_res["file"]
    LOG.info(
        "File: %s | Kích thước: %d bytes | %d chunk(s) | chunk_size: %d",
        file_meta["file_name"], file_meta["file_size"],
        file_meta["total_chunks"], file_meta["chunk_size"],
    )
    return tracker_res


def _find_missing_chunks(
    file_meta: dict[str, Any],
    dest_dir: Path,
) -> tuple[list[int], dict[int, dict[str, Any]]]:
    total_chunks = file_meta["total_chunks"]
    chunks_meta = {c["chunk_index"]: c for c in file_meta["chunks"]}
    missing: list[int] = []
    for idx in range(total_chunks):
        part_file = dest_dir / f"{idx}.part"
        if part_file.exists():
            # Xác thực lại file .part đã có (phòng file bị hỏng)
            data = part_file.read_bytes()
            exp = chunks_meta[idx]["chunk_hash"]
            if verify_chunk_integrity(data, exp):
                LOG.info("Chunk %d đã có và hợp lệ, bỏ qua.", idx)
                continue
            else:
                LOG.warning("Chunk %d đã có nhưng SHA-256 sai, sẽ tải lại.", idx)
        missing.append(idx)
    return missing, chunks_meta


def _build_chunk_peer_map(
    peers: list[dict[str, Any]],
) -> dict[int, list[tuple[str, int, str]]]:
    if not peers:
        raise DownloadError("Tracker báo không có Seeder nào online")
    chunk_to_peers: dict[int, list[tuple[str, int, str]]] = {}
    for peer in peers:
        host, port = peer["host"], peer["port"]
        peer_id = peer.get("peer_id", f"{host}:{port}")
        for chunk_index in peer.get("chunk_indexes", []):
            chunk_to_peers.setdefault(chunk_index, []).append((host, port, peer_id))
    return chunk_to_peers


def _download_chunk_from_peer(
    chunk_index: int,
    file_id: str,
    peer: tuple[str, int, str],
    expected_hash: str,
    expected_length: int,
    target_part: Path,
    tracker_address: tuple[str, int] | None,
    local_peer_id: str | None,
    timeout: float,
) -> bool:
    host, port, peer_id = peer
    try:
        LOG.info(
            "[Worker] Tải chunk %d từ Seeder %s (%s:%d)...",
            chunk_index, peer_id, host, port,
        )
        data = fetch_chunk_from_peer(
            (host, port), file_id, chunk_index,
            timeout=timeout, expected_length=expected_length,
        )
        if not verify_chunk_integrity(data, expected_hash, expected_length):
            LOG.warning(
                "[FAILOVER] Chunk %d từ Seeder %s sai SHA-256! Đang chuyển sang Seeder khác...",
                chunk_index, peer_id,
            )
            return False
        save_chunk(data, target_part)
        LOG.info(
            "[Worker] Chunk %d OK ← Seeder %s (%s:%d)",
            chunk_index, peer_id, host, port,
        )
        if local_peer_id and tracker_address:
            announce_chunk_to_tracker(
                tracker_address, local_peer_id, file_id, chunk_index, timeout=timeout
            )
        return True
    except DownloadError as err:
        LOG.warning(
            "[FAILOVER] Seeder %s (%s:%d) bị ngắt/timeout khi tải chunk %d: %s. Chuyển sang nguồn thay thế...",
            peer_id, host, port, chunk_index, err,
        )
        return False


def _refresh_chunk_peers(
    chunk_index: int,
    file_id: str,
    tracker_address: tuple[str, int],
    current_peers: list[tuple[str, int, str]],
    timeout: float,
) -> list[tuple[str, int, str]]:
    LOG.info(
        "[RETRY] Tất cả Seeder cho chunk %d đều lỗi. Đang hỏi lại Tracker danh sách mới...",
        chunk_index,
    )
    time.sleep(1.0)
    try:
        fresh_res = query_tracker(
            tracker_address, file_id, chunk_indexes=[chunk_index], timeout=timeout
        )
        fresh_peers = [
            (peer["host"], peer["port"], peer.get("peer_id", f"{peer['host']}:{peer['port']}"))
            for peer in fresh_res.get("peers", [])
            if chunk_index in peer.get("chunk_indexes", [])
        ]
    except DownloadError:
        return current_peers

    if not fresh_peers:
        return current_peers
    LOG.info(
        "[RETRY] Tracker cung cấp %d Seeder mới cho chunk %d",
        len(fresh_peers), chunk_index,
    )
    return fresh_peers


def _download_file_chunk(
    chunk_index: int,
    file_id: str,
    file_meta: dict[str, Any],
    chunks_meta: dict[int, dict[str, Any]],
    chunk_to_peers: dict[int, list[tuple[str, int, str]]],
    dest_dir: Path,
    tracker_address: tuple[str, int] | None = None,
    peer_id: str | None = None,
    timeout: float = 5.0,
    max_retries: int = 2,
) -> int:
    """Tải một chunk với cơ chế chịu lỗi (Fault Tolerance):  
    1. Thử lần lượt các Seeder trong danh sách.
    2. Nếu Seeder bị tắt/timeout, log [FAILOVER] và chuyển ngay sang Seeder khác.
    3. Nếu tất cả Seeder trong cache thất bại, tự động hỏi lại Tracker để tìm Seeder mới.
    4. Khi tải xong và verify hash thành công, announce lên Tracker nếu có peer_id.
    """
    exp_hash = chunks_meta[chunk_index]["chunk_hash"]
    exp_len = min(
        file_meta["chunk_size"],
        file_meta["file_size"] - chunk_index * file_meta["chunk_size"],
    )
    target_part = dest_dir / f"{chunk_index}.part"
    candidate_peers = list(chunk_to_peers.get(chunk_index, []))

    for attempt in range(max_retries + 1):
        for candidate in candidate_peers:
            if _download_chunk_from_peer(
                chunk_index, file_id, candidate, exp_hash, exp_len, target_part,
                tracker_address, peer_id, timeout,
            ):
                return chunk_index
        if attempt >= max_retries or tracker_address is None:
            break
        candidate_peers = _refresh_chunk_peers(
            chunk_index, file_id, tracker_address, candidate_peers, timeout
        )

    raise DownloadError(f"Tất cả Seeder cho chunk {chunk_index} đều thất bại sau {max_retries} lần thử.")


def _download_missing_chunks(
    missing: list[int],
    file_id: str,
    file_meta: dict[str, Any],
    chunks_meta: dict[int, dict[str, Any]],
    chunk_to_peers: dict[int, list[tuple[str, int, str]]],
    dest_dir: Path,
    max_workers: int,
    tracker_address: tuple[str, int] | None = None,
    peer_id: str | None = None,
    timeout: float = 5.0,
) -> None:
    orphans = [idx for idx in missing if not chunk_to_peers.get(idx)]
    if orphans:
        raise DownloadError(f"Không có Seeder nào giữ chunk: {orphans}")

    failed: list[int] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _download_file_chunk, idx, file_id, file_meta, chunks_meta,
                chunk_to_peers, dest_dir, tracker_address=tracker_address,
                peer_id=peer_id, timeout=timeout,
            ): idx
            for idx in missing
        }
        for future in concurrent.futures.as_completed(futures):
            chunk_index = futures[future]
            try:
                future.result()
            except DownloadError as err:
                LOG.exception("Chunk %d thất bại hoàn toàn: %s", chunk_index, err)
                failed.append(chunk_index)
    if failed:
        raise DownloadError(
            f"Không thể tải {len(failed)} chunk: {sorted(failed)}. "
            "Hãy thử lại sau hoặc kiểm tra Seeder."
        )


def download_full_file(
    file_id: str,
    output_dir: Path | str = "downloads",
    tracker_address: tuple[str, int] | None = None,
    peer_id: str | None = None,
    max_workers: int = 4,
    timeout: float = 10.0,
) -> Path:
    """Tải song song các chunk còn thiếu, sau đó ghép file và kiểm tra SHA-256."""
    tracker_address = _validate_full_file_request(file_id, tracker_address, max_workers)
    dest_dir = Path(output_dir) / file_id
    tracker_res = _get_full_file_response(file_id, tracker_address, timeout)
    file_meta = tracker_res["file"]
    total_chunks = file_meta["total_chunks"]
    missing, chunks_meta = _find_missing_chunks(file_meta, dest_dir)

    if missing:
        LOG.info("Cần tải %d/%d chunk: %s", len(missing), total_chunks, missing)
        chunk_to_peers = _build_chunk_peer_map(tracker_res.get("peers", []))
        _download_missing_chunks(
            missing, file_id, file_meta, chunks_meta, chunk_to_peers,
            dest_dir, max_workers, tracker_address=tracker_address, peer_id=peer_id, timeout=timeout,
        )
    else:
        LOG.info("Tất cả %d chunk đã có sẵn, chuyển sang ghép file.", total_chunks)

    return reassemble_file(file_id, file_meta["file_name"], total_chunks, dest_dir)

def reassemble_file(
    file_id: str,
    file_name: str,
    total_chunks: int,
    parts_dir: Path,
) -> Path:
    """Ghép các file .part (0.part, 1.part, ...) thành file hoàn chỉnh và kiểm tra SHA-256.
    Trả về Path của file hoàn chỉnh nếu hash khớp.
    Xóa file nếu hash không khớp và raise DownloadError.
    Xóa các file .part sau khi ghép thành công.
    """
    final_path = parts_dir / file_name
    hasher = hashlib.sha256()

    LOG.info("Bắt đầu ghép %d chunk → %s", total_chunks, final_path)

    # Ghi tuần tự từng chunk vào file đích, đồng thời tính SHA-256
    with final_path.open("wb") as out:
        for idx in range(total_chunks):
            part_file = parts_dir / f"{idx}.part"
            if not part_file.exists():
                raise DownloadError(f"Thiếu file chunk {part_file}")
            data = part_file.read_bytes()
            hasher.update(data)
            out.write(data)

    actual_hash = hasher.hexdigest()
    if actual_hash != file_id:
        final_path.unlink(missing_ok=True)
        raise DownloadError(
            f"SHA-256 toàn file không khớp!\n"
            f"  Kỳ vọng: {file_id}\n"
            f"  Thực tế: {actual_hash}"
        )

    LOG.info("SHA-256 toàn file khớp: %s", actual_hash)

    # Dọn dẹp file .part
    for idx in range(total_chunks):
        part_file = parts_dir / f"{idx}.part"
        part_file.unlink(missing_ok=True)
    LOG.info("Đã xóa %d file .part tạm.", total_chunks)

    LOG.info("[HOÀN THÀNH] File đã ghép thành công: %s", final_path)
    return final_path


def main() -> None:
    settings = load_settings()

    parser = argparse.ArgumentParser(
        description="Leecher: Download and verify chunks from Seeders",
    )
    parser.add_argument("file_id", help="SHA-256 hex of the file to download")
    parser.add_argument(
        "--chunk-index", type=int, default=None,
        help="Download only this chunk index (Week-1 mode). "
             "Omit to download the entire file (Week-2 mode).",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("downloads"),
                        help="Directory to save downloaded files")
    parser.add_argument("--tracker-host", default=settings.tracker_host,
                        help="Tracker host address")
    parser.add_argument("--tracker-port", type=int, default=settings.tracker_port,
                        help="Tracker port")
    parser.add_argument("--peer-host",
                        help="Direct Seeder host (bypass Tracker, Week-1 only)")
    parser.add_argument("--peer-port", type=int,
                        help="Direct Seeder port (bypass Tracker, Week-1 only)")
    parser.add_argument("--expected-hash",
                        help="Expected SHA-256 hex when downloading directly")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel download workers (default: 4)")
    parser.add_argument("--peer_id", help="Leecher peer_id for announcing chunks to Tracker")

    args = parser.parse_args()

    # Validation
    if bool(args.peer_host) != (args.peer_port is not None):
        parser.error("--peer-host and --peer-port must be used together")
    if args.peer_port is not None and not 1 <= args.peer_port <= 65535:
        parser.error("--peer-port must be between 1 and 65535")
    if not 1 <= args.tracker_port <= 65535:
        parser.error("--tracker-port must be between 1 and 65535")
    if args.workers < 1:
        parser.error("--workers must be at least 1")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    direct_addr = (
        (args.peer_host, args.peer_port)
        if args.peer_host and args.peer_port else None
    )
    tracker_addr: tuple[str, int] | None = (
        (args.tracker_host, args.tracker_port) if direct_addr is None else None
    )

    try:
        if args.chunk_index is not None:
            # ── Chế độ Week 1: Tải 1 chunk ──
            saved = download_single_chunk(
                file_id=args.file_id,
                chunk_index=args.chunk_index,
                output_dir=args.output_dir,
                tracker_address=tracker_addr,
                direct_peer_address=direct_addr,
                expected_hash=args.expected_hash,
            )
            print(f"\n[HOÀN THÀNH] Chunk {args.chunk_index}: {saved}")
        else:
            # ── Chế độ Week 2: Tải toàn bộ file ──
            if direct_addr is not None:
                parser.error(
                    "Full-file download requires Tracker. "
                    "Remove --peer-host/--peer-port or specify --chunk-index."
                )
            if tracker_addr is None:
                parser.error("Full-file download requires a Tracker address.")
            saved = download_full_file(
                file_id=args.file_id,
                output_dir=args.output_dir,
                tracker_address=tracker_addr,
                peer_id=args.peer_id,
                max_workers=args.workers,
            )
            print(f"\n[HOÀN THÀNH] File đã tải và xác thực: {saved}")
    except Exception as exc:
        LOG.exception("Quá trình tải thất bại: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
