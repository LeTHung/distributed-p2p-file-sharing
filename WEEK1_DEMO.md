# Nghiệm thu tích hợp tuần 1

Cần PostgreSQL local đang chạy, database/user đã có và `.env` điền đúng theo
`.env.example`. Chỉ Tracker truy cập database; schema tự khởi tạo khi chạy
Tracker. Mỗi người dùng database local của mình, không đưa `.env` lên Git.

Cài dependency từ thư mục gốc:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Mở ba terminal tại thư mục gốc repo. Để lưu log tiếng Việt trên Windows,
đặt `$env:PYTHONIOENCODING='utf-8'` trong mỗi terminal.

Terminal 1:

```powershell
.\.venv\Scripts\python.exe -m tracker.server
```

Terminal 2:

```powershell
.\.venv\Scripts\python.exe -m peer.uploader samples/README.md --peer-id seeder-hung --port 7001 --chunk-size 1024
```

Seeder in `file_id` (SHA-256 file mẫu). Chép giá trị đó vào lệnh terminal 3:

```powershell
.\.venv\Scripts\python.exe -m peer.downloader FILE_ID --chunk-index 0 --output-dir downloads
```

Phải có `downloads/FILE_ID/0.part`, bytes và SHA-256 khớp chunk 0 của file
nguồn. Thử chờ hơn 20 giây rồi tải lại: vẫn thành công nhờ heartbeat.
Ctrl+C Seeder để dừng và xóa đăng ký nguồn; tải lại phải báo không có nguồn.
Nếu tắt cưỡng bức, chờ hơn TTL và chu kỳ dọn trước khi kiểm tra nguồn đã mất.
Sau khi khởi động lại Seeder, nguồn phải được đăng ký và announce trở lại.

## Kiểm tra tự động

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Test PostgreSQL/E2E chỉ chạy khi đặt `TRACKER_TEST_DSN` (tài khoản test có
quyền tạo database) và `TRACKER_UPLOADER_SOURCE` là đường dẫn tuyệt đối tới
`peer/uploader.py`. Không ghi DSN chứa mật khẩu vào issue/PR. Mỗi bài test
tạo schema/database tạm riêng và tự dọn, không xóa dữ liệu ứng dụng.

Đặt `TRACKER_TEST_LOG_DIR` tới `data/week1-evidence` để giữ log. Bài E2E
chạy PostgreSQL, Tracker, Seeder và Leecher thật: khởi động lại Tracker,
chờ quá TTL (test dùng heartbeat 1 giây, TTL 6 giây, chờ 8 giây), xác nhận
nguồn còn sống và tải đúng chunk cuối ngắn bằng Leecher CLI. Log Leecher,
Tracker, Seeder và kết quả probe hash được giữ trong thư mục đã chọn.

Tuần 1 chỉ nghiệm thu một chunk qua ba module. Tải song song từ hai nguồn,
ghép file và failover giữa lúc tải vẫn là các mốc sau. Worker pool tuần 2 ở
PR riêng; không dùng kết quả tuần 1 để xác nhận các mốc đó đã hoàn thành.
