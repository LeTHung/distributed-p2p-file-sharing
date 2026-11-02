# Seeder tuần 1 — Lê Tiến Hưng

Phạm vi: chia file, SHA-256 toàn file và từng chunk, đăng ký với Tracker,
phục vụ `get_chunk` qua TCP. Server xử lý tuần tự, một request mỗi kết nối.
Worker pool, throttling và heartbeat để các tuần sau.

## Chạy

Từ thư mục gốc dự án, cài `pip install -r requirements.txt`, cấu hình `.env`
theo README rồi chạy khi Tracker đã sẵn sàng:

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-hung --port 7001 --chunk-size 1024
```

Chạy thử riêng khi chưa có Tracker:

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-hung --skip-tracker
python -m unittest discover -s tests -v
```

`--skip-tracker` chỉ phục vụ kiểm tra P2P; không chứng minh được luồng tích hợp
Tracker → Seeder → Leecher. File ID được in trong log. Khi chạy LAN, dùng
`--host 0.0.0.0 --advertise-host <IP-LAN>` để Tracker lưu địa chỉ kết nối được.
Giữ file nguồn nguyên trạng trong suốt phiên chia sẻ; khởi động lại nếu sửa file.
File rỗng bị từ chối vì schema hiện tại yêu cầu `total_chunks > 0`.

## Hợp đồng tích hợp

Mỗi request có UUID `request_id`; phản hồi cần trả cùng ID và `ok: true`.
Seeder gọi theo thứ tự, mỗi request mở một kết nối TCP:

1. `register_peer`: `peer_id`, `host`, `port`.
2. `register_file`: `file_id`, `file_name`, `file_size`, `chunk_size`,
   `total_chunks`, `chunks: [{chunk_index, chunk_hash}]`.
3. `announce_chunks`: `peer_id`, `file_id`, `chunk_indexes`.

**Điểm cần chốt với Huỳnh:** kế hoạch cũ để `announce_chunks` sang tuần 2,
nhưng giao thức dùng request này để nối peer với chunk. Bản Seeder gọi API
này để Tracker có đủ dữ liệu trả `get_peers`; nếu Tracker chưa hỗ trợ thì
khởi động sẽ báo lỗi rõ ràng. Không tự thêm `peer_id` vào `register_file`.
Heartbeat chưa triển khai nên chưa phù hợp Tracker bắt buộc gia hạn TTL.

Leecher gửi `type: get_chunk`, `request_id`, `file_id`, `chunk_index` (từ 0).
Header thành công gồm `request_id`, `ok`, `file_id`, `chunk_index`,
`data_length`, sau đó đúng `data_length` byte nhị phân. Header dùng framing
4 byte unsigned big-endian của `common.framing`. Leecher đối chiếu SHA-256
với `chunk_hash` trong metadata, không tìm hash trong header truyền chunk.
Lỗi chỉ trả JSON gồm `ok: false`, `error_code`, `message`, `request_id`.

## Kiểm tra đã có

Test tự động mở socket TCP thật trên localhost: tải tất cả chunk gồm chunk
cuối ngắn, so hash và ghép lại; file/chunk không tồn tại; request sai;
file nguồn thay đổi; file rỗng; đăng ký metadata với Tracker giả lập.
Tracker và Leecher thật chưa có trong repo nên vẫn cần nhóm chạy tích hợp
ba module để nghiệm thu mốc cuối tuần 1.

Nguồn đối chiếu: README hiện tại và `docs/WORK_PLAN.md`, `docs/PROTOCOL.md`,
`docs/TEAM_RULES.md` tại commit `43357b6`. Tài khoản Drive kết nối báo 404
với thư mục nhóm, vì vậy chưa xác minh được tài liệu Drive có cập nhật mới.
