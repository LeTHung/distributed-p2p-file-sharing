# BitTorrent Lite Hybrid P2P File Sharing

Đồ án cuối kỳ môn Các hệ thống phân tán INT1405 - Nhóm 6.

Hệ thống cho phép nhiều peer cùng chia sẻ một file theo từng chunk. Tracker trung tâm chỉ quản lý metadata và directory peer/chunk bằng PostgreSQL; dữ liệu chunk truyền trực tiếp giữa các peer qua TCP.

## Phân công

| Thành viên | Vai trò | Phạm vi chính |
|---|---|---|
| Lương Thị Như Huỳnh | Student 1 - Tracker | Tracker, PostgreSQL, metadata, heartbeat, directory và tích hợp |
| Lê Tiến Hưng | Student 2 - Seeder/Uploader | Chia file, hash, đăng ký file/chunk, peer server, worker và throttling |
| Võ Nguyễn Bảo Hưng | Student 3 - Leecher/Downloader | Truy vấn peer, tải song song, xác minh hash, failover và ghép file |

Tài liệu kế hoạch, quy tắc, giao thức và demo được lưu trên [Google Drive của nhóm](https://drive.google.com/drive/u/0/folders/1eXti4kdNvFuTGrBXi3ccLNkYgyXzfiJ6). GitHub là nguồn chính của code, issue và pull request.

## Công nghệ đã chốt

- Python 3.11+
- TCP socket, JSON UTF-8 và length prefix 4 byte
- `threading` cho xử lý đồng thời
- PostgreSQL và `psycopg`
- Nhiều tiến trình chạy trên cùng một máy khi demo

## Cấu trúc code

```text
common/      # framing TCP, cấu hình, metadata và heartbeat client dùng chung
tracker/     # Student 1 triển khai Tracker và lớp truy cập PostgreSQL
peer/        # Student 2 và Student 3 triển khai Seeder và Leecher
database/    # Schema PostgreSQL
tests/       # Test tự động cho các thành phần dùng chung
samples/     # Chỉ file mẫu nhỏ, vô hại
docs/        # Protocol TCP/JSON, handover Tracker tuần 3–4 và hướng dẫn demo
```

## Khởi động môi trường

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Tạo database `p2p_tracker`, điền thông tin kết nối trong `.env`, rồi khởi tạo schema bằng `python -m tracker.server --init-db-only`. Lệnh này gọi migration idempotent trong `database/schema.sql`; không cần chạy SQL thủ công. Không commit `.env`, password, token hoặc file demo lớn.

## Hợp đồng kỹ thuật không được tự ý đổi

- `file_id` là SHA-256 hex chữ thường của toàn bộ nội dung file.
- `chunk_index` bắt đầu từ `0`; chunk cuối có thể ngắn hơn `chunk_size`.
- Header của mọi thông điệp TCP là 4 byte unsigned big-endian, biểu diễn độ dài JSON UTF-8 đi sau.
- Request Tracker: `register_peer`, `register_file`, `announce_chunks`, `get_peers`, `heartbeat`, `unregister_peer`, `list_files`.
- Request giữa peer: `get_chunk`.
- Chỉ Tracker được kết nối PostgreSQL. Peer không truy cập database trực tiếp.

## Cách làm việc

1. Lấy issue được giao theo tuần trên GitHub.
2. Cập nhật `develop` bằng `git pull --ff-only origin develop`, rồi tạo branch `feature/<mo-ta>` hoặc `fix/<mo-ta>` từ `develop`.
3. Mỗi pull request chỉ làm một việc, ghi rõ cách chạy và kết quả kiểm tra.
4. Có ít nhất một thành viên khác review trước khi merge; Lê Tiến Hưng quyết định và thực hiện merge.
5. PR feature vào `develop`; sau nghiệm thu tích hợp mới đưa `develop` vào `main`. Không push trực tiếp hai nhánh này.

Mốc tuần 1 là: Leecher hỏi Tracker, tải được một chunk từ Seeder và kiểm tra SHA-256 đúng.

## Leecher tuần 1

Sau khi Tracker và Seeder đã chạy, lấy `file_id` từ log Seeder rồi tải một chunk:

```powershell
python -m peer.downloader <file_id> --chunk-index 0 --output-dir downloads
```

Kết quả nằm ở `downloads/<file_id>/0.part`, chỉ lưu sau khi đúng độ dài và SHA-256.
Leecher nhận được cả metadata top-level của Tracker hiện tại lẫn trường `file`
theo hợp đồng Drive. Nếu nguồn lỗi hoặc hash sai, thử peer tiếp theo trong danh
sách đã nhận; chưa có tải song song/ghép toàn file hoặc hỏi lại Tracker khi hết nguồn.

Thử riêng với Seeder bằng `--peer-host 127.0.0.1 --peer-port 7001`
và bắt buộc `--expected-hash <chunk_hash>`; thiếu hash sẽ bị từ chối.
Seeder gửi heartbeat theo cấu hình (mặc định 5 giây), nên vẫn được tìm thấy
sau TTL 20 giây khi đang chạy. Ctrl+C dừng phục vụ và unregister; nếu bị tắt
cưỡng bức, Tracker loại peer theo TTL. Khi heartbeat lỗi, Seeder thử đăng ký
và announce lại toàn bộ nguồn. Không xem test tải một chunk là nghiệm thu đa nguồn.

Xem [kịch bản nghiệm thu tuần 1](WEEK1_DEMO.md) để chạy ba terminal và lưu log.

## Heartbeat và kiểm tra tuần 3–4

Tracker dùng `HEARTBEAT_INTERVAL_SECONDS=5`, `PEER_TTL_SECONDS=20` và `TRACKER_CLEANUP_INTERVAL_SECONDS=5` theo `.env.example`. Các truy vấn directory loại peer hết TTL ngay cả khi cleanup nền chưa chạy. Cleanup xóa peer cùng `peer_chunks` theo cascade, còn metadata file/chunk vẫn được giữ.

Sau khi `register_peer` thành công, peer có thể dùng `common.heartbeat.PeerHeartbeat` để gửi heartbeat theo cùng giao thức TCP/JSON. Lỗi `PEER_NOT_FOUND` nghĩa là peer hết TTL hoặc không còn được đăng ký: thành phần quản lý peer phải đăng ký lại rồi công bố lại toàn bộ chunk ownership hiện có. Helper chỉ gửi heartbeat; nó không tự đoán endpoint hoặc ownership. Callback chạy trên heartbeat thread, vì vậy hãy đưa công việc phục hồi vào hàng đợi/lifecycle worker thay vì thực hiện nhiều RPC đồng bộ trong callback.

Student 2's current Seeder already has its own `SeederRegistration` lifecycle: it retries `register_peer`, `register_file` and `announce_chunks` after a Tracker RPC failure, and unregisters on normal shutdown. Student 3's current Downloader fetches one chunk and does not host chunks, so it has no peer heartbeat lifecycle yet.

Tracker tự khởi tạo schema khi chạy `python -m tracker.server --init-db-only`. Hướng dẫn chạy và phạm vi demo hiện có nằm trong [docs/DEMO_GUIDE.md](docs/DEMO_GUIDE.md); kết quả kiểm tra và các phụ thuộc tích hợp nằm trong [docs/TRACKER_WEEK3_4.md](docs/TRACKER_WEEK3_4.md).

Hợp đồng TCP/JSON hiện tại, gồm framing, RPC, metadata, lỗi và các khác biệt implementation đã biết, được ghi tại [docs/PROTOCOL.md](docs/PROTOCOL.md). Tài liệu này phải được cập nhật cùng `04_Giao_thuc_TCP_JSON.docx` theo quy tắc nhóm.

Full suite mới nhất (9/11/2026): **80 passed, 0 failed, 0 skipped**. Các mốc trước đó được giữ trong handover: 76 trước T01, 75 trước sửa thời gian chờ shutdown heartbeat, 74 trước E2E phục hồi Uploader, và 70 là mốc lịch sử. Kết quả và giới hạn tích hợp được ghi trong [docs/TRACKER_WEEK3_4.md](docs/TRACKER_WEEK3_4.md), lệnh chạy nằm trong [docs/DEMO_GUIDE.md](docs/DEMO_GUIDE.md).
