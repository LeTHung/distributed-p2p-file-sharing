# Bắt đầu ở đây

Tài liệu này dành cho cả ba thành viên trước khi viết code. Mục tiêu của tuần đầu không phải là làm xong từng module, mà là chứng minh ba module có thể giao tiếp được với nhau.

## 1. Chuẩn bị một lần

1. Clone repo và tạo branch riêng từ `main`.
2. Cài Python 3.11+ và PostgreSQL 16+.
3. Tạo virtual environment, cài `requirements.txt`.
4. Sao chép `.env.example` thành `.env`; không commit `.env`.
5. Tạo database `p2p_tracker`, chạy `database/schema.sql`.
6. Đọc `ARCHITECTURE.md`, `PROTOCOL.md`, `TEAM_RULES.md` và `WORK_PLAN.md`.

## 2. Cách chạy demo khi hoàn thành

Mỗi tiến trình chạy trong một terminal riêng, với port và thư mục dữ liệu khác nhau:

```text
Terminal 1: Tracker       127.0.0.1:9000
Terminal 2: Seeder A      127.0.0.1:7001, data/seeder-a/
Terminal 3: Seeder B      127.0.0.1:7002, data/seeder-b/
Terminal 4: Leecher       127.0.0.1:7003, data/leecher/
```

Lệnh chính xác sẽ được cập nhật vào README sau khi tracker và peer entrypoint được tạo. Không tự thay đổi protocol để “chạy được một mình”.

## 3. Quy trình nhận việc

1. Mở issue có nhãn tuần và vai trò của mình.
2. Tạo branch theo tên issue, ví dụ `feature/uploader-register-file`.
3. Làm một phần nhỏ có thể chạy được; commit bằng câu ngắn, rõ nghĩa.
4. Mở pull request, ghi cách chạy và kết quả kiểm tra.
5. Một người khác review rồi mới merge vào `main`.

## 4. Khi bị kẹt

Gửi vào nhóm ba thông tin: câu lệnh đã chạy, log lỗi ngắn, branch/commit hiện tại và điều mày mong đợi. Không gửi `.env`, password hoặc token.

## 5. Tiêu chí “xong” của một việc

Một issue chỉ hoàn thành khi: code ở branch, có kiểm tra phù hợp, PR đã merge, README/tài liệu được cập nhật nếu thay đổi cách chạy hoặc giao thức, và người khác clone `main` vẫn chạy được.
