# Chạy với PostgreSQL local

Tracker là tiến trình duy nhất kết nối PostgreSQL. Seeder và Leecher chỉ kết
nối Tracker và các peer qua TCP, không cần tài khoản database.

## Chuẩn bị trên Windows

Cài Python 3.11+, PostgreSQL và khởi động service PostgreSQL. Từ thư mục gốc
repo trên branch có script này:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m scripts.setup_local
```

Nhập mật khẩu quản trị PostgreSQL tại terminal; mặc định tài khoản `postgres`
và cổng `5432`. Không gửi mật khẩu lên chat, issue hoặc PR. Dùng
`--admin-user TEN --port CONG` nếu khác mặc định.

Script tạo role riêng `p2p_app` (không phải superuser), database `p2p_tracker`,
khởi tạo schema và ghi `.env` local. Mật khẩu app mới được tạo ngẫu nhiên;
mật khẩu quản trị không được lưu. Nếu role app đã tồn tại, dùng mật khẩu từ
`.env` hoặc yêu cầu nhập, không tự đổi mật khẩu role hiện có. Database có
owner khác sẽ bị từ chối thay vì chiếm quyền hoặc xóa dữ liệu. Nếu đã có
`.env`, giữ các giá trị hiện có và lưu bản dự phòng trong `data/local-setup/`.
Các thư mục runtime và `.env` đã được gitignore.

## Kiểm tra local tự động

```powershell
.\.venv\Scripts\python.exe -m scripts.check_local
```

Script tự chọn cổng trống, chạy Tracker và Seeder bằng hai tiến trình riêng,
rồi chạy probe tải toàn bộ file mẫu, xác minh hash từng chunk và toàn file.
Kết quả nằm trong `data/local-check/result.json`; log cùng thư mục. Khi xong,
chỉ dừng các tiến trình do script tạo và unregister peer test. Metadata mẫu
được giữ lại trong database để chạy kiểm tra lại. Không dùng Docker.

Probe hiện là công cụ kiểm tra, không phải module Leecher của Bảo Hưng.
Khi module Leecher được tích hợp, dùng lệnh của module đó để nghiệm thu ba
module thật. Không coi probe thành bằng chứng tải đa nguồn hoặc failover.

## Chạy module riêng

Terminal Tracker:

```powershell
.\.venv\Scripts\python.exe -m tracker.server
```

Terminal Seeder:

```powershell
.\.venv\Scripts\python.exe -m peer.uploader samples/README.md --peer-id seeder-hung --port 7001 --chunk-size 1024
```

Terminal Leecher: dùng entrypoint của Bảo Hưng khi có trong `develop`.
Cả ba dùng cùng `TRACKER_HOST=127.0.0.1`, `TRACKER_PORT=9000`; mỗi Seeder có
peer_id/port riêng. Chỉ máy chạy Tracker cần tạo database.

**Giới hạn hiện tại:** Tracker đã bật TTL mặc định 20 giây, Seeder tuần 1–2
chưa gửi heartbeat. Vì vậy nguồn có thể biến mất sau 20 giây. Script kiểm
tra local tăng TTL lên 3600 giây riêng cho Tracker con, không đổi `.env`
hoặc service đang chạy. Đây là cấu hình smoke test tạm thời, không thay thế
heartbeat; phải đồng bộ vòng đời peer trước demo dài/chịu lỗi.

Nếu ba thành viên phát triển độc lập trên ba máy, mỗi người có thể dùng quy
trình này cho database local của mình. Nếu muốn các peer ở ba máy kết nối
cùng một hệ thống, cần cấu hình IP LAN cho Tracker/peer thay cho loopback;
PostgreSQL vẫn chỉ cần ở máy Tracker. Thiết lập hiện tại chỉ dùng loopback,
không mở PostgreSQL ra mạng.
