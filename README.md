# BitTorrent-Lite: Hybrid P2P File Sharing

Đồ án cuối kỳ môn Các hệ thống phân tán (INT1405), Track A.

Hệ thống cho phép nhiều peer chia sẻ một file theo từng chunk. Tracker trung tâm chỉ lưu metadata và directory peer; dữ liệu chunk truyền trực tiếp giữa các peer qua TCP.

## Mục tiêu bắt buộc

- Chia file thành các chunk có kích thước cố định, dùng SHA-256 để kiểm tra từng chunk.
- Tracker quản lý peer, file, chunk, heartbeat và loại peer hết hạn.
- Seeder phục vụ nhiều yêu cầu tải chunk đồng thời.
- Leecher tìm peer qua tracker, tải song song, kiểm tra hash, ghép file và đổi nguồn khi lỗi.
- Demo nhiều tiến trình trên cùng một máy, gồm ít nhất Tracker, 2 Seeder và 1 Leecher.

## Công nghệ

Python 3.11+, TCP socket, JSON với length-prefix 4 byte, `threading`, PostgreSQL và `psycopg`.

## Cấu trúc dự kiến

```text
src/
  common/        # Protocol, framing, metadata, hash, cấu hình chung
  tracker/       # Tracker server và lớp truy cập PostgreSQL
  peer/          # Peer server (uploader) và client (downloader)
database/        # Schema và script khởi tạo database
docs/            # Thiết kế, giao thức, quy tắc và kế hoạch
```

## Bắt đầu

1. Cài Python 3.11+ và PostgreSQL 16+.
2. Tạo môi trường ảo: `python -m venv .venv`.
3. Kích hoạt môi trường ảo và chạy `pip install -r requirements.txt`.
4. Sao chép `.env.example` thành `.env`, rồi điền cấu hình PostgreSQL.
5. Tạo database và chạy `database/schema.sql`.

Chi tiết kiến trúc và giao thức nằm tại [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) và [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Cách làm việc

Đọc [docs/TEAM_RULES.md](docs/TEAM_RULES.md) trước khi nhận việc. Kế hoạch và trách nhiệm từng tuần nằm tại [docs/WORK_PLAN.md](docs/WORK_PLAN.md).
