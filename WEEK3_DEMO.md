# Demo tuần 3

Chuẩn bị PostgreSQL local/.env theo WEEK1_DEMO.md. Bản này gồm toàn bộ tuần
2, upload pacing, retry/refresh nguồn và chia sẻ chunk Leecher đã kiểm tra.
Mỗi lệnh server dưới chạy trong terminal riêng, tại thư mục gốc repo.

Tạo file mẫu 256 KiB và phần đuôi ngắn, không commit runtime:

```powershell
python -c "from pathlib import Path; p=Path('data/week3-demo.bin'); p.parent.mkdir(exist_ok=True); p.write_bytes(bytes(range(256))*1024+b'tail')"
```

Tracker và hai Seeder:

```powershell
python -m tracker.server
```

```powershell
python -m peer.uploader data/week3-demo.bin --peer-id aaa-first --port 7001 --chunk-size 65536 --workers 2 --upload-rate 65536
```

```powershell
python -m peer.uploader data/week3-demo.bin --peer-id bbb-backup --port 7002 --chunk-size 65536 --workers 2 --upload-rate 262144
```

Leecher dùng file_id từ log, tạo endpoint chia sẻ riêng:

```powershell
python -m peer.downloader FILE_ID --peer-id leecher-a --serve-port 7003 --workers 2 --output-dir downloads/a
```

Khi Seeder A có log Upload started nhưng chưa Sent chunk, tắt tiến trình
Seeder A. Có thể Ctrl+C để hủy upload/unregister, hoặc tắt cưỡng bức để thử
TTL. Leecher nhận body thiếu/lỗi socket, log [FAILOVER], chuyển sang B và
hoàn thành đủ file với SHA-256 đúng. Không khẳng định mất bytes đúng tại thời
điểm tắt nếu thao tác chậm và file đã tải xong; chạy lại từ thư mục output mới
để tránh resume bỏ qua các chunk vốn đã có.

File cuối ở downloads/a/FILE_ID/completed/week3-demo.bin. Tiến trình Leecher
tiếp tục chạy để chia sẻ, heartbeat và retry công bố chunk nếu announce lỗi
tạm thời. Địa chỉ phục vụ phải khác Seeder. Mặc định chỉ bind loopback; khi
chạy LAN dùng --serve-host 0.0.0.0 --advertise-host IP_LAN cho Leecher và cấu
hình Tracker/Seeder tương ứng. Không công bố 0.0.0.0 làm địa chỉ nguồn.

Sau khi A dừng, dừng B, đợi quá TTL 20 giây và chu kỳ dọn 5 giây (nếu tắt
cưỡng bức). Giữ Leecher A đang chạy. Từ một terminal khác:

```powershell
python -m peer.downloader FILE_ID --workers 2 --output-dir downloads/b
```

Peer mới phải nhận được toàn file từ leecher-a. So Get-FileHash của hai file
đầu ra với data/week3-demo.bin: cả ba giống nhau. Đây là bằng chứng Leecher
đã trở thành nguồn dữ liệu thực sự, không chỉ có hàng announce trong DB.
Ctrl+C Leecher A để dừng và unregister; Tracker không còn trả nguồn đó.

Nếu không có nguồn còn sống, Leecher hỏi lại Tracker hữu hạn rồi báo chờ
nguồn, giữ .part hợp lệ. Khi nguồn khởi động lại, chạy lại lệnh để resume.
Không có lựa chọn chờ vô hạn. Hạ chunk_size/tăng upload rate khi tổng thời
gian pacing vượt deadline nhận body; rate là tổng cho Seeder, không phải mỗi
client. Xem peer/UPLOADER_WEEK3.md để biết giới hạn fairness/bandwidth.

## Bằng chứng tự động

`python -m unittest discover -s tests -v` chạy các test. PostgreSQL/E2E cần
TRACKER_TEST_DSN và TRACKER_UPLOADER_SOURCE như WEEK1_DEMO.md. Khi đặt
TRACKER_TEST_LOG_DIR, test tuần 3 giữ week3-result.json và log các tiến trình.
Test dùng DB tạm riêng, chỉ tự dọn dữ liệu test; không commit DSN hoặc .env.

Test week3 E2E đợi header của A, tắt A trước khi body hoàn tất, xác minh
failover đúng file, kiểm tra các chunk công bố bởi Leecher có cổng thật,
đợi TTL loại các Seeder gốc, tải lại toàn file chỉ từ Leecher, rồi gửi
Ctrl+C/Ctrl+Break và xác nhận unregister. Test dùng TTL 6 giây/heartbeat 1
giây để chạy nhanh, không thay cấu hình mặc định trong .env của ứng dụng.

Các phần chuẩn bị slide, video bảo vệ và diễn tập toàn nhóm vẫn thuộc tuần
4. Một số test/helper từ PR Tracker tuần 3–4 có trong repo không tự đồng
nghĩa các issue nghiệm thu tuần 4 đã hoàn thành.
