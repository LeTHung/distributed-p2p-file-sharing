# Kế hoạch 4 tuần

Kế hoạch bắt đầu từ Thứ Hai, 26/10/2026. Nếu thầy có hạn nộp sớm hơn, giữ nguyên mốc tích hợp nhưng rút ngắn việc phụ, không bỏ mốc cuối tuần 1.

| Tuần | Khoảng ngày | Mục tiêu chung có thể kiểm tra | Lương Thị Như Huỳnh - Tracker | Lê Tiến Hưng - Seeder | Võ Nguyễn Bảo Hưng - Leecher |
|---|---|---|---|---|---|
| 1 | 26/10 - 01/11 | Tải được 1 chunk qua Tracker -> Seeder -> Leecher | Tạo DB, schema, Tracker `register_peer`, `register_file`, `get_peers`; viết framing dùng chung | Chia file, tính hash, `register_file`, mở server và trả 1 chunk | Đọc metadata, gọi `get_peers`, xin 1 chunk, kiểm tra hash và lưu `.part` |
| 2 | 02/11 - 08/11 | Tải xong file nhỏ; 2 Seeder phục vụ đồng thời | Hoàn thiện `announce_chunks`, `list_files`, xử lý truy vấn đồng thời | Worker pool/dispatcher, phục vụ nhiều request; thông báo chunk hiện có | Tải song song nhiều chunk từ 2 nguồn; ghép file và so hash file hoàn chỉnh |
| 3 | 09/11 - 15/11 | Tắt một Seeder giữa lúc tải mà Leecher vẫn hoàn thành nếu còn nguồn khác | `heartbeat`, job dọn peer quá hạn, transaction và xử lý lỗi PostgreSQL | Throttling cơ bản; xử lý peer shutdown/khởi động lại và đăng ký lại | Timeout, retry, chọn peer khác; announce chunk vừa tải để thành peer nguồn |
| 4 | 16/11 - 22/11 | Demo hoàn chỉnh và bảo vệ được từng phần | Kịch bản demo, tích hợp, logging, review tài liệu | Test tải nhiều client, chỉnh lỗi và giải thích uploader policy | Test lỗi/hash, hướng dẫn chạy, ảnh/chứng cứ demo và slide phần downloader |

## Điều kiện hoàn thành từng tuần

- **Cuối tuần 1:** Có video/log chứng minh Leecher nhận một chunk đúng hash qua đủ ba module.
- **Cuối tuần 2:** File nhận được có SHA-256 toàn file bằng file gốc, và log thể hiện chunk đến từ ít nhất hai Seeder.
- **Cuối tuần 3:** Tắt một Seeder khi đang tải; log thể hiện Leecher timeout rồi chuyển nguồn, Tracker loại peer theo TTL.
- **Cuối tuần 4:** Chạy lệnh theo README trên máy sạch hoặc thư mục mới; cả nhóm diễn tập demo và mỗi người giải thích phần mình.
