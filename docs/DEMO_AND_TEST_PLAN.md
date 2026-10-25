# Kế hoạch kiểm thử và demo

## Dữ liệu mẫu

- Dùng file text hoặc binary 5-20 MB để dễ thấy nhiều chunk nhưng không làm demo quá lâu.
- Dùng một `chunk_size` cố định trong demo, ví dụ 1 MiB.
- File thật không commit nếu lớn; lưu trong Drive nhóm. Chỉ commit file text nhỏ vô hại khi cần test tự động.
- Trước demo ghi SHA-256 file nguồn; sau khi ghép phải in SHA-256 file đích để so sánh.

## Kịch bản demo chính

1. Khởi động PostgreSQL và Tracker.
2. Khởi động Seeder A với đủ chunk của file mẫu; nó đăng ký peer, file và chunk với Tracker.
3. Khởi động Seeder B với một tập con chunk hoặc chuẩn bị để tải một phần từ A.
4. Dùng `list_files` chứng minh Tracker thấy file; dùng `get_peers` chứng minh Tracker trả nguồn theo chunk.
5. Khởi động Leecher. Leecher tải các chunk khác nhau đồng thời từ A và B.
6. Hiện log hash đúng của ít nhất hai chunk, rồi hash file ghép đúng file nguồn.
7. Bắt đầu một lượt tải khác; tắt Seeder A giữa chừng. Leecher phải timeout, hỏi lại Tracker và lấy phần còn thiếu từ B nếu B có.
8. Chờ TTL. Tracker không còn trả Seeder A trong `get_peers`.

## Bảng kiểm thử tối thiểu

| Mã | Tình huống | Kết quả mong đợi |
|---|---|---|
| T01 | JSON header bị thiếu/truncated | Đóng kết nối an toàn, log `INVALID_REQUEST` |
| T02 | Xin chunk không tồn tại | Seeder trả `CHUNK_NOT_FOUND` |
| T03 | Hash chunk sai | Leecher xóa `.part`, không announce chunk đó |
| T04 | Peer không heartbeat | Tracker xóa peer sau TTL và cascade `peer_chunks` |
| T05 | 2 request tới Seeder cùng lúc | Cả hai nhận phản hồi, không block lẫn nhau |
| T06 | Seeder đang tải bị tắt | Leecher retry peer khác hoặc báo chờ nguồn rõ ràng |
| T07 | Cùng file đăng ký lại | Không tạo trùng metadata/chunk; thao tác idempotent |

## Vai trò khi bảo vệ

- **Huỳnh:** giải thích soft-state, database, directory, heartbeat và transaction/cascade.
- **Hưng:** giải thích chia file, dispatcher/worker, giới hạn upload, luồng gửi chunk.
- **Bảo Hưng:** giải thích scheduling tải song song, hash, retry/failover, ghép file.
- Cả ba phải giải thích được đường đi: Leecher -> Tracker -> Seeder -> Leecher.
