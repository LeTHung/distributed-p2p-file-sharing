# Kết quả kiểm thử Seeder tuần 4

Ngày chạy: 16/11/2026 (Asia/Saigon). Môi trường: Windows 11,
Python 3.13.7, PostgreSQL 18 local cho bộ regression/E2E.

## Tải đồng thời qua TCP loopback

```powershell
.\.venv\Scripts\python.exe -m peer.uploader_bench --clients 24 --requests 256 --workers 4 --max-pending 64
```

| Chỉ số | Kết quả |
| --- | ---: |
| Request xác minh SHA-256 thành công | 256/256 |
| Request thất bại / bị từ chối | 0 / 0 |
| Payload được client xác minh | 3.948.694 bytes |
| Thời gian workload | 0,294977 giây |
| Worker hoạt động cao nhất | 4/4 |
| Connection đang xử lý và chờ cao nhất | 22/68 |
| Connection còn hoạt động hoặc chờ sau kết thúc | 0 |

Report gốc: `data/week4-evidence/20261115T182555Z-6b3dfd9a/report.json`.
Tên thư mục dùng UTC; cùng thư mục có `uploader.log` và file nguồn mẫu.

## Giới hạn băng thông tổng

```powershell
.\.venv\Scripts\python.exe -m peer.uploader_bench --clients 4 --requests 8 --workers 2 --max-pending 8 --chunk-size 8192 --upload-rate 65536 --timeout 15
```

8/8 request xác minh đúng, không lỗi hoặc bị từ chối. Tổng payload 65.536
bytes, thời gian 1,157506 giây với giới hạn tổng 65.536 bytes/giây.
Đỉnh 2 worker và 4 connection đang xử lý/chờ; sau kết thúc còn 0.
Report gốc: `data/week4-evidence/20261115T182632Z-9306b895/report.json`.

## Regression và tình huống lỗi

`python -m unittest discover -s tests -v`: **109 test đạt trong 113,403 giây**,
không skip, có PostgreSQL thật và E2E failover. Log local:
`data/week4-evidence/full-suite/tests.log`.

Bốn test tải mới kiểm tra nhiều client tải đúng hash, trộn request hợp lệ và
không tìm thấy file, client gửi thiếu header bị timeout, cùng tình huống đầy
worker/queue. Sau timeout hoặc quá tải, pool tiếp tục phục vụ request mới;
các connection đã nhận đều được kết thúc và giải phóng capacity.

## Phạm vi bằng chứng

Benchmark trên dùng nhiều client thread và TCP loopback trên một máy, đo riêng
Uploader; không cần Tracker. Thời gian là số đo của lần chạy này, không phải
cam kết hiệu năng trên LAN hoặc ba máy. Bộ E2E riêng kiểm tra tích hợp và failover.

File nguồn, report chi tiết và log nằm trong `data/` được gitignore; tài liệu này
lưu bản tóm tắt có thể tái lập. Chạy lại lệnh để tạo bằng chứng trên máy khác.
Chưa bao gồm video, slide hoặc buổi diễn tập của nhóm. Cách demo và giải thích
dispatcher/worker xem [UPLOADER_WEEK4.md](../peer/UPLOADER_WEEK4.md).
