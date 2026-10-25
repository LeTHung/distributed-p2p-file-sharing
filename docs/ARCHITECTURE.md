# Kiến trúc hệ thống

## Thành phần

```text
Seeder A ─┐                         ┌─> PostgreSQL
Seeder B ─┼─ TCP + JSON ─> Tracker ─┤    peers, files, chunks, peer_chunks
Leecher  ─┘                         └─> directory peer/chunk

Leecher ───────── TCP + JSON ─────────> Seeder A / Seeder B
                 (truyền bytes chunk trực tiếp)
```

- **Tracker:** không lưu bytes nội dung file. Nó xác thực request cơ bản, lưu directory, trả các peer còn hoạt động cho mỗi chunk, và xóa peer quá hạn heartbeat.
- **Seeder/Uploader:** tính `file_id`, chia file theo `chunk_size`, thông báo metadata và chunk sở hữu với Tracker; sau đó lắng nghe yêu cầu `get_chunk`.
- **Leecher/Downloader:** lấy metadata và danh sách nguồn từ Tracker; tải chunk song song, kiểm tra SHA-256, lưu phần đã đúng, announce chunk mới có và ghép file khi đủ.
- **PostgreSQL:** chỉ Tracker truy cập. Peer không được kết nối thẳng database.

## Quy ước cốt lõi

- `file_id` là SHA-256 của toàn bộ nội dung file ở dạng hex chữ thường.
- `chunk_index` bắt đầu từ `0`; chunk cuối có thể ngắn hơn `chunk_size`.
- SHA-256 hash chunk ở dạng hex chữ thường.
- Một peer có thể đồng thời là leecher và seeder nếu nó đã có các chunk hợp lệ.
- Mỗi tiến trình local phải có `peer_id`, thư mục dữ liệu và port riêng.

## Xử lý mất kết nối

Peer gửi heartbeat mỗi 5 giây. Nếu Tracker không thấy peer trong 20 giây, Tracker xóa peer này; do khóa ngoại `ON DELETE CASCADE`, những dòng `peer_chunks` liên quan cũng bị xóa. Khi tải chunk bị timeout hoặc lỗi socket, Leecher hỏi lại Tracker và chọn nguồn khác. Nếu không có peer nào có chunk còn thiếu, lượt tải dừng ở trạng thái chờ nguồn.
