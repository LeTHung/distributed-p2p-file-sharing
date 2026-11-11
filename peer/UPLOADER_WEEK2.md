# Seeder tuần 2 — Lê Tiến Hưng

Issue #5: phục vụ ít nhất hai request chunk đồng thời mà một kết nối chậm
không chặn toàn bộ server. Không thay đổi JSON, framing hay thuật toán hash.

## Cách hoạt động

Main thread nhận kết nối rồi giao cho `ThreadPoolExecutor`. `--workers`
(mặc định 4) giới hạn số kết nối được xử lý cùng lúc. `--max-pending`
(mặc định 16) giới hạn thêm số kết nối được nhận và chờ worker. Mỗi worker
đọc một request, mở file riêng, đọc/kiểm tra hash và gửi chunk, rồi đóng socket
và trả slot kể cả khi lỗi. Không dùng chung con trỏ file giữa các worker.

Khi hết cả slot xử lý và slot chờ, server đóng kết nối mới và ghi log, không
đọc request hoặc gửi header thành công. Client có thể gặp EOF/reset và cần
thử lại/chọn nguồn khác. Không trả `RATE_LIMITED` với request_id giả vì lúc
này chưa đọc request. Đây là giới hạn tài nguyên worker/queue; giới hạn tốc
độ upload, heartbeat, retry/restart tự động thuộc tuần 3.

Mỗi kết nối đang xử lý có timeout theo `SOCKET_TIMEOUT_SECONDS`. Thời gian
chờ queue không tính vào timeout socket của worker; Leecher phải có timeout
riêng và có thể hết hạn trước khi đến lượt. Dừng server bằng Ctrl+C sẽ đóng
cổng nghe và chờ các request đã nhận hoàn thành hoặc gặp timeout. Nếu dùng
API trong code: gọi `shutdown()` từ thread khác để dừng `serve_forever()`,
rồi `server_close()` để đợi worker và giải phóng socket.

## Chạy một Seeder

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-hung --port 7001 --chunk-size 1024 --workers 4 --max-pending 16
```

Tracker cần hỗ trợ `register_peer`, `register_file`, `announce_chunks` theo
tài liệu giao thức. Seeder vẫn announce toàn bộ index đã có sau khi đăng ký
metadata thành công. Chưa có Tracker thì thêm `--skip-tracker` để thử riêng;
cách này không công bố nguồn và không chứng minh luồng tích hợp.

## Chuẩn bị hai nguồn cho Leecher

Chạy cùng file nguồn và cùng chunk_size trong hai terminal:

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-a --port 7001 --chunk-size 1024 --workers 4
python -m peer.uploader samples/README.md --peer-id seeder-b --port 7002 --chunk-size 1024 --workers 4
```

Hai Seeder có cùng file_id và metadata nhưng peer_id/port khác nhau.
Tracker lưu cả hai nguồn; Leecher chọn chunk từ cả hai. Không sửa file nguồn
khi đang chạy. Đây là hướng dẫn tích hợp, chưa phải bằng chứng nghiệm thu.

## Kiểm tra

```powershell
python -m unittest discover -s tests -v
```

Test TCP localhost gồm hai worker cùng đi qua barrier trong lúc đọc chunk
(server tuần tự không qua được), 12 client tải từ pool 3 worker và đối chiếu
SHA-256 từng chunk, client gửi thiếu header, hết capacity rồi phục hồi, lỗi
handler không làm mất worker, cùng các test tuần 1.

Mốc chung tuần 2 vẫn cần Tracker/Leecher thật: tải đủ file từ ít nhất hai
Seeder, ghép đúng thứ tự và so SHA-256 toàn file. PR Seeder chỉ xác nhận phần
worker pool; có thành viên khác review trước khi Lê Tiến Hưng merge vào
`develop`. Chỉ đưa `develop` vào `main` sau nghiệm thu tích hợp.
