# Seeder tuần 3 — Lê Tiến Hưng

Issue #8: giới hạn upload/worker, shutdown và đăng ký lại sau restart.
Bản này tích hợp worker pool từ phần tuần 2 (#5, PR #15 chưa merge tại thời
điểm bắt đầu), giữ heartbeat/đăng ký lại đã có trên develop. Không thay đổi
framing, metadata hay header get_chunk. Seeder vẫn đọc từ toàn bộ file gốc.

## Chạy

Tracker PostgreSQL local phải sẵn sàng; cấu hình .env theo README.

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-hung --port 7001 --chunk-size 65536 --workers 4 --max-pending 16 --upload-rate 131072
```

- `--workers 4`: tối đa 4 kết nối đang xử lý, bao gồm đọc request/gửi chunk.
- `--max-pending 16`: tối đa 16 kết nối chờ worker; vượt capacity đóng socket
  mới và log. Chưa đọc request nên không giả lập request_id để trả RATE_LIMITED.
- `--upload-rate 131072`: tổng bytes payload mỗi giây cho cả Seeder, tức
  128 KiB/s, chia sẻ giữa mọi worker; JSON control không tính vào giới hạn.
- `--upload-rate 0` (mặc định): không giới hạn tốc độ; vẫn giới hạn worker/queue.

Pacer chia bytes thành block nhỏ, đợi trước mỗi lần gửi, dùng chung một lock
để hai worker không mỗi bên tiêu thụ toàn bộ tốc độ. Không có burst khi mới
kết nối hoặc tích lũy credit lúc idle. Đây là giới hạn tối đa bảo thủ; tốc độ
thực có thể thấp hơn do socket, disk, lịch thread. Chưa cam kết công bằng
băng thông giữa các client. Memory chứa tối đa một chunk cho mỗi worker.

## Demo tốc độ

File samples/README.md quá nhỏ để nhìn rõ giới hạn. Tạo file demo riêng:

```powershell
python -c "from pathlib import Path; p=Path('data/week3-demo.bin'); p.parent.mkdir(exist_ok=True); p.write_bytes(bytes(range(256))*1024)"
python -m peer.uploader data/week3-demo.bin --peer-id seeder-hung --port 7001 --chunk-size 65536 --workers 4 --upload-rate 131072
```

Lấy file_id từ log, tải một chunk bằng Leecher tuần 1:

```powershell
python -m peer.downloader FILE_ID --chunk-index 0 --output-dir downloads
```

Chunk 64 KiB dùng ít nhất khoảng 0,5 giây pacing; hai chunk tổng 128 KiB
qua hai client dùng ít nhất khoảng 1 giây, không phải mỗi client đều có
128 KiB/s riêng. Log có bytes, elapsed, rate_limit và request_id. SHA-256
vẫn phải đúng. Có thể đổi `--upload-rate 0` rồi chạy lại để so sánh, không
kỳ vọng chính xác tốc độ đo được của lần tải rất nhỏ.

**Timeout:** chọn chunk_size và tốc độ phù hợp. Leecher hiện có deadline
nhận body mặc định 5 giây. Khi tổng dữ liệu chờ/gửi của các client quá lớn
so với upload-rate, Leecher có thể timeout dù Seeder đang pacing đúng.
Hạ chunk_size, giảm số client hoặc tăng tốc độ; không tắt timeout. Timeout
socket phía Seeder chỉ áp dụng thao tác mạng, không tính thời gian đợi pacer.

## Shutdown và restart

Ctrl+C (SIGINT; SIGTERM trên hệ điều hành hỗ trợ) dừng nhận việc, hủy upload
đang chạy, ngắt socket đang chờ và đánh thức worker đang pacing. Request
đã nhận header thành công nhưng bị hủy sẽ có body thiếu; Leecher phải coi
đó là tải lỗi và không lưu chunk vừa nhận. Dừng heartbeat, unregister và
đợi thread giải phóng tài nguyên. Worker chờ queue không được xử lý tiếp
khi đã có stop. Không đợi gửi hết file ở tốc độ rất thấp mới cho thoát.

Tắt cưỡng bức không thể unregister; Tracker lọc nguồn theo TTL và dọn các
quan hệ peer_chunks. Chạy lại đúng file, peer_id, port và chunk_size: Seeder
tính lại metadata, register_peer, register_file và announce toàn bộ index.
File không đổi có cùng file_id; không sinh metadata/chunk trùng. Nếu file
đổi, file_id mới được đăng ký; không dùng hash cũ cho bytes mới.

Nếu Tracker tạm mất kết nối hoặc peer hết hạn, heartbeat lỗi sẽ kích hoạt
đăng ký/announce lại vào lần tick sau; lỗi tiếp tục sẽ tiếp tục thử theo
interval cấu hình. Đây là khôi phục đăng ký, không khẳng định đã có downloader
failover tự động khi đang tải. Không chạy hai Seeder cùng peer_id: peer_id
là định danh một tiến trình đang hoạt động.

## Kiểm tra

```powershell
python -m unittest discover -s tests -v
```

Test TCP thật kiểm tra aggregate rate qua hai client, bytes chính xác, chunk
cuối, chế độ unlimited, restart cùng port và ngắt upload 1 B/s không bị kẹt.
Test pool kiểm tra concurrency, queue, phục hồi slot. Test PostgreSQL/E2E
cần TRACKER_TEST_DSN và TRACKER_UPLOADER_SOURCE theo WEEK1_DEMO.md; dùng
database tạm riêng và tự dọn. E2E giữ nguồn sống quá TTL, restart Tracker,
tắt cưỡng bức Seeder, xác nhận nguồn hết hạn, bật lại cùng ID/port và kiểm
tra announce đủ chunk rồi tải bằng Leecher CLI thật.

Chỉ nhóm trưởng review/quyết định merge vào develop. Không merge đồng thời
PR #15 cũ với bản tuần 3 này mà chưa đối chiếu phần worker pool trùng nhau.
Phần Tracker và Leecher tuần 2/3 có PR riêng, không tự merge trong tác vụ này.
