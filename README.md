# BitTorrent Lite Hybrid P2P File Sharing

Đồ án cuối kỳ môn Các hệ thống phân tán INT1405 - Nhóm 6.

Hệ thống cho phép nhiều peer cùng chia sẻ một file theo từng chunk. Tracker trung tâm chỉ quản lý metadata và directory peer/chunk bằng PostgreSQL; dữ liệu chunk truyền trực tiếp giữa các peer qua TCP.

## Phân công

| Thành viên | Vai trò | Phạm vi chính |
|---|---|---|
| Lương Thị Như Huỳnh | Student 1 - Tracker | Tracker, PostgreSQL, metadata, heartbeat, directory và tích hợp |
| Lê Tiến Hưng | Student 2 - Seeder/Uploader | Chia file, hash, đăng ký file/chunk, peer server, worker và throttling |
| Võ Nguyễn Bảo Hưng | Student 3 - Leecher/Downloader | Truy vấn peer, tải song song, xác minh hash, failover và ghép file |

Tài liệu kế hoạch, quy tắc, giao thức và demo được lưu trên [Google Drive của nhóm](https://drive.google.com/drive/u/0/folders/1eXti4kdNvFuTGrBXi3ccLNkYgyXzfiJ6). GitHub là nguồn chính của code, issue và pull request.

## Công nghệ đã chốt

- Python 3.11+
- TCP socket, JSON UTF-8 và length prefix 4 byte
- `threading` cho xử lý đồng thời
- PostgreSQL và `psycopg`
- Nhiều tiến trình chạy trên cùng một máy khi demo

## Cấu trúc code

```text
common/      # framing TCP, cấu hình và các tiện ích dùng chung
tracker/     # Student 1 triển khai Tracker và lớp truy cập PostgreSQL
peer/        # Student 2 và Student 3 triển khai Seeder và Leecher
database/    # Schema PostgreSQL
tests/       # Test tự động cho các thành phần dùng chung
samples/     # Chỉ file mẫu nhỏ, vô hại
```

## Khởi động môi trường

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Tạo database `p2p_tracker`, điền thông tin vào `.env`, rồi chạy `database/schema.sql`. Không commit `.env`, password, token hoặc file demo lớn.

Có thể dùng [hướng dẫn PostgreSQL local](LOCAL_SETUP.md) và chạy
`python -m scripts.setup_local` để tạo database/user, `.env` và schema;
`python -m scripts.check_local` kiểm tra Tracker–Seeder bằng probe tải chunk.
Mật khẩu quản trị được nhập tại terminal, không ghi vào code.

## Hợp đồng kỹ thuật không được tự ý đổi

- `file_id` là SHA-256 hex chữ thường của toàn bộ nội dung file.
- `chunk_index` bắt đầu từ `0`; chunk cuối có thể ngắn hơn `chunk_size`.
- Header của mọi thông điệp TCP là 4 byte unsigned big-endian, biểu diễn độ dài JSON UTF-8 đi sau.
- Request Tracker: `register_peer`, `register_file`, `announce_chunks`, `get_peers`, `heartbeat`, `unregister_peer`, `list_files`.
- Request giữa peer: `get_chunk`.
- Chỉ Tracker được kết nối PostgreSQL. Peer không truy cập database trực tiếp.

## Cách làm việc

1. Lấy issue được giao theo tuần trên GitHub.
2. Tạo branch `feature/<mo-ta>` từ `main`.
3. Mỗi pull request chỉ làm một việc, ghi rõ cách kiểm tra.
4. Có một thành viên khác review trước khi merge.
5. Không push trực tiếp lên `main`.

Mốc tuần 1 là: Leecher hỏi Tracker, tải được một chunk từ Seeder và kiểm tra SHA-256 đúng.
