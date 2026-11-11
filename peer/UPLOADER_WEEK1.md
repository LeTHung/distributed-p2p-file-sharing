# Seeder tuần 1 — Lê Tiến Hưng

Phạm vi: chia file, SHA-256 toàn file và từng chunk, đăng ký với Tracker,
phục vụ `get_chunk` qua TCP. Bản tuần 1 xử lý tuần tự, một request mỗi kết nối.
Đã bổ sung heartbeat và unregister để tích hợp ổn định với TTL của Tracker.
Trên branch tuần 3 đã tích hợp worker pool và upload policy; xem
[hướng dẫn tuần 3](UPLOADER_WEEK3.md) cho bản hiện tại.

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
Seeder hiện từ chối file rỗng; Tracker đã hỗ trợ metadata file rỗng, nhưng truyền file rỗng không thuộc demo tuần 1.

## Hợp đồng tích hợp

Mỗi request có UUID `request_id`; phản hồi cần trả cùng ID và `ok: true`.
Seeder gọi theo thứ tự, mỗi request mở một kết nối TCP:

1. `register_peer`: `peer_id`, `host`, `port`.
2. `register_file`: `file_id`, `file_name`, `file_size`, `chunk_size`,
   `total_chunks`, `chunks: [{chunk_index, chunk_hash}]`.
3. `announce_chunks`: `peer_id`, `file_id`, `chunk_indexes`.

Tracker đã có announce_chunks và phản hồi get_peers chứa metadata trong file, đồng thời giữ trường top-level để tương thích. Seeder heartbeat mỗi 5 giây theo cấu hình; nếu lỗi sẽ thử đăng ký và announce lại. Ctrl+C unregister; tắt cưỡng bức được dọn theo TTL.

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
Đã có Tracker và Leecher thật. Xem [kịch bản nghiệm thu](../WEEK1_DEMO.md) để chạy ba module, kiểm tra sau TTL và lưu log. Giao thức đối chiếu theo tài liệu Drive nhóm đã truy cập được; bản Git cũ tại commit 43357b6 chỉ là lịch sử.
