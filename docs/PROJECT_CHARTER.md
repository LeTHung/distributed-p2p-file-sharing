# Project charter - BitTorrent-Lite

## Sản phẩm nhóm phải trình bày

Một hệ thống chia sẻ file P2P lai gồm Tracker và nhiều Peer chạy đồng thời trên một máy. Tracker giữ directory peer/chunk bằng PostgreSQL; peer truyền dữ liệu chunk trực tiếp với nhau qua TCP.

## Phạm vi bắt buộc

- Seeder tạo metadata: `file_id`, kích thước file, `chunk_size`, số chunk và SHA-256 từng chunk.
- Tracker nhận đăng ký peer/file/chunk, `heartbeat`, truy vấn `get_peers`, `list_files`, và loại peer hết hạn.
- Leecher tải nhiều chunk song song từ nhiều Seeder, xác minh hash từng chunk và ghép file đúng thứ tự.
- Khi một Seeder lỗi, Leecher thử nguồn thay thế còn hoạt động.
- Seeder phục vụ đồng thời và có upload policy cơ bản: giới hạn số request/chunk upload đang hoạt động trên mỗi peer hoặc tổng số worker.
- Có kịch bản demo và log chứng minh các yêu cầu trên.

## Ngoài phạm vi giai đoạn đầu

- GUI web/desktop, đăng nhập người dùng, mã hóa đầu cuối, NAT traversal và chạy nhiều máy thật.
- Redis, Docker, Kubernetes, cloud deployment, microservice framework.

Chỉ bổ sung các phần này sau khi demo bắt buộc chạy ổn định.

## Tiêu chí nghiệm thu cuối

| Tình huống | Bằng chứng cần có |
|---|---|
| Đăng ký file | Tracker có metadata đúng và `chunks` đủ từ 0 đến `total_chunks - 1` |
| Directory | `get_peers` chỉ trả peer active và đúng chunk peer sở hữu |
| Tải song song | Log cho thấy ít nhất hai kết nối Seeder phục vụ các chunk khác nhau |
| Toàn vẹn | SHA-256 từng chunk và SHA-256 file sau ghép khớp file nguồn |
| Fault tolerance | Tắt Seeder giữa lúc tải, Leecher timeout rồi chuyển nguồn khác nếu có |
| Soft state | Tracker loại peer sau TTL, dòng `peer_chunks` liên quan cũng biến mất |
| Concurrency | Seeder không bị treo khi nhận ít nhất hai request đồng thời |

## Quyết định đã chốt

Python, TCP Socket, JSON, length prefix 4 byte, `threading`, PostgreSQL, `psycopg`, demo nhiều tiến trình local; `file_id` là SHA-256 toàn file, `chunk_index` bắt đầu từ 0.
