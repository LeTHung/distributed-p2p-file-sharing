# Quy tắc làm việc nhóm

## Phân công chính

| Thành viên | Vai trò chính | Phạm vi sở hữu |
|---|---|---|
| Lương Thị Như Huỳnh | Student 1 - Tracker & tích hợp | `src/tracker/`, `src/common/` dùng chung, PostgreSQL, metadata, heartbeat, ghép nhánh và demo tổng |
| Lê Tiến Hưng | Student 2 - Seeder/Uploader | `src/peer/uploader*`, đọc chunk, xử lý worker, đăng ký file/chunk, upload throttling |
| Võ Nguyễn Bảo Hưng | Student 3 - Leecher/Downloader | `src/peer/downloader*`, lập lịch tải song song, xác minh hash, failover, ghép file |

Mỗi người có trách nhiệm hỗ trợ tích hợp và hiểu luồng toàn hệ thống; phạm vi sở hữu xác định người chịu trách nhiệm chính khi có lỗi.

## Git workflow

1. `main` luôn phải chạy được. Không code trực tiếp lên `main`.
2. Mỗi việc tạo một branch: `feature/tracker-heartbeat`, `feature/uploader-server`, `feature/downloader-parallel` hoặc `fix/<mô-tả>`.
3. Mỗi pull request chỉ làm một việc rõ ràng, có mô tả cách chạy và kết quả đã kiểm tra.
4. Có ít nhất một thành viên khác xem PR trước khi merge; người tạo PR không tự merge khi chưa được review.
5. Không commit `.env`, password PostgreSQL, file mẫu lớn hoặc thư mục runtime. Dùng `.env.example` và dữ liệu nhỏ vô hại.
6. Trước khi làm tiếp mỗi ngày: `git pull origin main`; trước khi mở PR: chạy các lệnh kiểm tra liên quan.

## Quy tắc kỹ thuật

- Giao thức trong `docs/PROTOCOL.md` là hợp đồng chung. Muốn sửa phải tạo issue/PR tài liệu trước hoặc cùng sửa với code phụ thuộc.
- Không đổi tên trường JSON, đánh số chunk hay thuật toán hash một phía.
- Không cho peer truy cập PostgreSQL trực tiếp; chỉ Tracker kết nối database.
- Mọi timeout, lỗi socket và lỗi database phải được log với `request_id` hoặc `peer_id`.
- Trước mỗi buổi tích hợp, chạy một file mẫu nhỏ và so SHA-256 của file gốc với file ghép lại.

## Nhịp làm việc

- Họp ngắn 2 lần/tuần: báo đã làm, đang kẹt, việc kế tiếp.
- Mỗi người cập nhật issue/PR trước buổi họp.
- Cuối tuần phải có bản chạy được theo mốc trong `WORK_PLAN.md`, không chỉ hoàn thành code riêng lẻ.
