# Leecher tuần 4 — Võ Nguyễn Bảo Hưng

Tài liệu tổng hợp hướng dẫn chạy sạch, kiểm thử lỗi/hash (T03, T06),
chứng cứ demo và nội dung giải thích cơ chế Downloader cho mốc Tuần 4.

## 1. Chạy kiểm thử tự động (Unit & E2E)

Từ thư mục gốc repo (đã cài `.venv`), chạy toàn bộ unit test của Downloader:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_downloader -v
```

Các kịch bản kiểm thử trọng tâm:
- **Tải song song & ghép file**: `test_download_parallel_from_two_seeders` kiểm tra chia tải qua nhiều Seeder và ghép file khớp SHA-256 gốc.
- **T03 — Chunk sai SHA-256**: `test_t03_corrupt_chunk_deletes_part_and_skips_announce` và `test_reassemble_file_detects_hash_mismatch` kiểm tra Leecher từ chối dữ liệu sai hash, xóa `.part` hỏng và **không** gọi `announce_chunks`.
- **T06 — Seeder tắt giữa chừng / hết nguồn**: `test_seeder_shutdown_mid_download_failover` kiểm tra failover sang Seeder còn lại; `test_t06_all_seeders_down_reports_waiting_for_source` kiểm tra báo lỗi rõ ràng và không tạo file đích khi toàn bộ nguồn mất.
- **Resume an toàn**: `test_download_full_file_resumes_valid_chunks` bỏ qua các `.part` đã hợp lệ và chỉ tải phần còn thiếu.

Nếu chạy kèm PostgreSQL (kiểm tra E2E thật Tracker → Seeder → Leecher):

```powershell
$env:TRACKER_TEST_DSN = 'host=127.0.0.1 port=5432 dbname=tracker_test user=tracker_test'
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_week3_e2e.py" -v
```

## 2. Hướng dẫn chạy demo trên máy sạch

### Chuẩn bị môi trường
1. Đảm bảo PostgreSQL đang chạy và đã cấu hình `.env` từ `.env.example`.
2. Tạo một file mẫu kích thước 5–20 MB (không commit lên Git), ví dụ `.\samples\demo_10mb.bin`:

```powershell
fsutil file createnew .\samples\demo_10mb.bin 10485760
# Hoặc dùng bất kỳ file nhị phân/văn bản 5–20 MB nào và ghi lại SHA-256 nguồn:
Get-FileHash .\samples\demo_10mb.bin -Algorithm SHA256
```

### Khởi chạy hệ thống (4 terminal)

**Terminal 1 — Khởi tạo schema (lần đầu) và chạy Tracker:**
```powershell
.\.venv\Scripts\python.exe -m tracker.server --init-db-only
.\.venv\Scripts\python.exe -m tracker.server
```

**Terminal 2 — Khởi động Seeder A (cổng 7001, `chunk_size` 1 MiB):**
```powershell
.\.venv\Scripts\python.exe -m peer.uploader .\samples\demo_10mb.bin --peer-id seed-a --port 7001 --chunk-size 1048576
```
*(Ghi lại `file_id` được in ra trên log của Seeder A.)*

**Terminal 3 — Khởi động Seeder B (cổng 7002):**
```powershell
.\.venv\Scripts\python.exe -m peer.uploader .\samples\demo_10mb.bin --peer-id seed-b --port 7002 --chunk-size 1048576
```

**Terminal 4 — Khởi động Leecher tải toàn bộ file song song và chia sẻ lại:**
```powershell
.\.venv\Scripts\python.exe -m peer.downloader <file_id> --workers 4 --output-dir downloads --peer-id leecher-1 --serve-port 7003
```
*(Bỏ `--peer-id` và `--serve-port` nếu chỉ muốn tải xong rồi thoát mà không giữ tiến trình chia sẻ lại.)*

### Kiểm tra kết quả sau khi tải
File hoàn chỉnh được lưu tại `downloads\<file_id>\demo_10mb.bin`. Đối chiếu SHA-256 với file nguồn:

```powershell
Get-FileHash .\downloads\<file_id>\demo_10mb.bin -Algorithm SHA256
```
Kết quả mong đợi: mã `Hash` trùng khớp 100% với `.\samples\demo_10mb.bin`, và log Leecher thể hiện các chunk được tải từ cả `seed-a` và `seed-b`.

## 3. Kịch bản kiểm chứng lỗi khi demo

1. **T06 — Tắt Seeder A giữa lúc đang tải**:
   - Dùng `--upload-rate` thấp trên Seeder hoặc tải file lớn hơn, sau đó nhấn `Ctrl+C` tắt Terminal 2 (`seed-a`) khi Leecher đang tải.
   - Quan sát log Leecher: xuất hiện `[FAILOVER]` / `[RETRY]`, Leecher chuyển sang `seed-b` và vẫn ghép xong file đúng SHA-256.
   - Chờ hết TTL (mặc định 20s): Tracker tự loại `seed-a` khỏi `get_peers`.
2. **T03 — Sai hash chunk**:
   - Đã chứng minh tự động qua `test_t03_corrupt_chunk_deletes_part_and_skips_announce` (Leecher xóa `.part` lỗi và không gọi `announce_chunks`).

## 4. Tóm tắt kiến trúc Downloader (phần bảo vệ)

1. **Lập lịch song song (`_build_chunk_peer_map`, `_download_missing_chunks`)**:
   - Xoay vòng danh sách nguồn theo `chunk_index % len(sources)` để phân tán tải đều giữa các Seeder thay vì dồn vào Seeder đầu tiên.
   - Dùng `ThreadPoolExecutor(max_workers=...)` tải đồng thời nhiều chunk độc lập.
2. **Xác minh toàn vẹn 2 lớp**:
   - **Tầng chunk**: `verify_chunk_integrity` kiểm tra độ dài byte và `SHA-256` trước khi ghi `.part` và trước khi `announce_chunks` lên Tracker.
   - **Tầng file**: `reassemble_file` ghép theo thứ tự `0 .. total_chunks - 1` vào file tạm rồi mới kiểm tra `SHA-256` toàn file trước khi thay thế file đích.
3. **Chịu lỗi & Failover (`_download_file_chunk`, `_refresh_chunk_peers`)**:
   - Khi một Seeder timeout, ngắt kết nối hoặc trả chunk sai hash, worker thử ngay Seeder tiếp theo trong danh sách.
   - Khi danh sách nguồn hiện tại đều thất bại, Leecher gọi lại `query_tracker` để lấy danh sách peer mới nhất (loại trừ chính `peer_id` của mình).
4. **Resume an toàn (`_find_missing_chunks`)**:
   - Khi chạy lại vào cùng thư mục `downloads\<file_id>`, Leecher kiểm tra lại `SHA-256` của từng `.part` đang có: chunk hợp lệ được giữ lại, `.part` hỏng bị xóa để tải lại.