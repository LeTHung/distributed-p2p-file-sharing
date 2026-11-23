# Seeder tuần 4 — Lê Tiến Hưng

Issue #11: kiểm thử nhiều request/client, kiểm tra upload policy, chuẩn bị
giải thích dispatcher/worker và log minh chứng. Chỉ hoàn thành phần Seeder;
runbook/report/video/slide của cả nhóm còn có issue #10 và #12 riêng.

## Chạy lại kiểm tra tải

Từ thư mục gốc `develop`, cài dependency và chạy:

```powershell
.\.venv\Scripts\python.exe -m peer.uploader_bench --clients 24 --requests 256 --workers 4 --max-pending 64
```

Công cụ tự tạo file binary mẫu 262.154 byte gồm chunk cuối ngắn, mở một
UploaderServer TCP loopback trên cổng trống và tối đa 24 client gửi tổng
256 request. Mỗi request mở kết nối riêng, có UUID, đối chiếu header, độ dài
và SHA-256 từng chunk. Server và các client dùng cùng tiến trình nhưng
khác thread, trao đổi thật qua socket; không gọi hàm đọc chunk thay cho mạng.
Không cần PostgreSQL vì bài này tách riêng uploader để đo.

Chạy thêm pacing tổng:

```powershell
.\.venv\Scripts\python.exe -m peer.uploader_bench --clients 4 --requests 8 --workers 2 --max-pending 8 --chunk-size 8192 --upload-rate 65536
```

8 request đầu mỗi request 8 KiB, tổng 64 KiB. Ở tổng rate 64 KiB/s, thời
gian nhận phải ít nhất xấp xỉ 1 giây dù có nhiều client. Có overhead nên
không yêu cầu chính xác bằng 1 giây. Có thể dùng --source FILE để đo một
file có sẵn, --timeout để đặt deadline request hoặc --output-dir để đổi
nơi lưu bằng chứng. Công cụ không sửa file nguồn được chỉ định.

Các request chia thành batch có tối đa số client cấu hình; không hứa mọi
client đều active ở mọi thời điểm. Không đặt capacity thấp hơn concurrency
nếu muốn mọi request thành công. Có thể chủ động giảm --max-pending về 0
để quan sát overload; EOF/reset và exit code 1 lúc đó là kết quả tải vượt
capacity, không tự động retry để che số request bị từ chối.

## Đọc báo cáo

Mỗi lần chạy có thư mục UTC timestamp + UUID riêng dưới data/week4-evidence:

- report.json: config, nguồn mẫu, checks, thống kê, kết quả/hash của từng request.
- uploader.log: request_id nối request với log gửi chunk; cuối file có summary.
- source.bin: file mẫu của lần chạy, nếu không chỉ định --source.

`passed=true` và exit code 0 chỉ khi tất cả request đúng hash, active worker
không vượt workers, số kết nối nhận/chờ không vượt workers+max_pending,
đã trả hết kết nối và không vượt pacing tổng. Exit code 1 nghĩa là một trong
các điều kiện trên không đạt; exit code 2 là cấu hình hoặc I/O không chạy được.
Không chỉ nhìn throughput rồi bỏ qua failed_requests hoặc checks.

Counter của server được bảo vệ bằng lock:

| Counter | Ý nghĩa |
|---|---|
| accepted_connections | Kết nối được nhận vào pool hoặc queue |
| rejected_connections | Kết nối bị đóng do hết capacity hoặc server đang dừng |
| active_connections / peak_active_connections | Worker đang xử lý / số cao nhất quan sát được |
| in_flight / peak_in_flight | Tổng kết nối đang chờ và đang xử lý / số cao nhất |
| finished_connections | Công việc đã trả slot, bao gồm request lỗi hoặc bị hủy |
| successful_chunks | Server đã gửi xong payload chunk |
| completed_payload_bytes | Bytes thuộc chunk gửi hoàn tất; không cộng payload dở dang hoặc header |
| error_responses | JSON response lỗi, gồm cả FILE_NOT_FOUND/CHUNK_NOT_FOUND |
| connection_errors | Timeout, đóng kết nối sớm hoặc lỗi handler |
| cancelled_connections | Kết nối/công việc bị hủy khi shutdown |

Server gửi xong không chứng minh client nhận đúng; `verified_requests` và
hash trong report là kiểm tra độc lập bên client. Server counters trong
benchmark được lấy sau shutdown/đợi worker nên không còn công việc nền.
CLI Seeder thông thường cũng in `Uploader summary` khi dừng, không thêm
request mới hoặc endpoint quản trị vào giao thức TCP.

## Nói về dispatcher và worker

1. Main thread nhận TCP connection, thử lấy slot không chờ. Không đọc/gửi
   chunk trên vòng accept nên client chậm không chặn việc nhận kết nối mới.
2. ThreadPoolExecutor có số worker cố định. BoundedSemaphore giới hạn tổng
   việc đang chạy và chờ; vượt giới hạn thì đóng socket, ghi log. TCP backlog
   riêng được đặt phù hợp capacity, tránh bottleneck mặc định 5 lúc client dồn tới.
3. Worker nhận đúng một JSON frame, kiểm tra request và đọc đoạn file bằng
   seek(index × chunk_size). Mỗi worker mở file riêng nên không tranh con trỏ.
4. So hash với metadata trước khi gửi. Header có data_length rồi đến bytes.
   Pacer dùng chung cho cả Seeder để giới hạn tổng bytes/giây, không cấp
   toàn bộ rate riêng cho từng worker.
5. finally đóng socket, kết thúc counters rồi trả slot. Shutdown đánh thức
   pacer, ngắt socket và chờ worker thoát; heartbeat/unregister quản lý nguồn
   trên Tracker theo phần tuần 3.

Giải thích tradeoff: rate 0 không pacing nhưng vẫn giới hạn worker/queue;
rate thấp và chunk lớn/nhiều client có thể vượt deadline Leecher. Policy
hiện tại bảo thủ, không cam kết chia đều băng thông. Mỗi worker giữ một chunk
trong RAM nên tăng workers/chunk_size làm tăng bộ nhớ. Benchmark loopback
một máy không chứng minh tốc độ LAN, Internet hoặc khả năng phục vụ vô hạn.

## Chạy demo tích hợp

WEEK1_DEMO.md giải thích luồng một chunk; WEEK2_DEMO.md tải hai nguồn;
WEEK3_DEMO.md kiểm tra upload pacing, tắt nguồn giữa body, failover và tải
tiếp từ Leecher đã chia sẻ. Dùng các kịch bản đó để trình bày hệ thống phân
tán; dùng benchmark tuần 4 để chứng minh riêng khả năng uploader.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Test uploader stress bổ sung 128 request/24 client, request hợp lệ xen kẽ
lỗi, client gửi thiếu header rồi timeout, queue đầy và phục hồi slot. Các
test PostgreSQL/E2E cần biến môi trường theo WEEK1_DEMO.md; log được giữ ở
TRACKER_TEST_LOG_DIR. Không ghi mật khẩu/DSN vào log, issue hay video.

Bổ sung nghiệm thu: bốn client tải đồng thời tổng 64 KiB với rate chung
64 KiB/s phải mất ít nhất xấp xỉ một giây, phát hiện việc cấp nhầm rate riêng
cho từng worker. Test dừng server giữ một client gửi thiếu frame và một client
đang chờ; `request_stop()` phải ngắt socket, hủy cả hai công việc, trả counters
về 0 mà không phải chờ socket timeout 30 giây. Chạy riêng bộ stress:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_uploader_stress.py -v
```

Khi quay video phần Seeder, nên lần lượt chỉ config, log Upload started /
Sent chunk, report verified_requests/failed_requests, peak worker/capacity,
so pacing tổng và kết quả failover. Không coi việc có tool và log là đã quay
xong video hoặc diễn tập bảo vệ cả nhóm.
