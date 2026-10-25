# Giao thức TCP

Mọi thông điệp điều khiển dùng TCP và JSON UTF-8 theo khung:

```text
[4 bytes unsigned big-endian: số byte JSON] [JSON bytes]
```

Thông điệp phản hồi thành công cho `get_chunk` gồm JSON header theo khung trên, sau đó là đúng `data_length` byte dữ liệu nhị phân. Vì vậy bên nhận luôn biết điểm kết thúc header và số byte phải đọc.

Mọi request có trường `type` và `request_id` (UUID). Mọi phản hồi có `request_id`, `ok` và, khi lỗi, `error_code`, `message`.

## Request Tracker

| `type` | Bên gửi | Mục đích | Trường chính |
|---|---|---|---|
| `register_peer` | Seeder/Leecher | Khai báo peer đang online | `peer_id`, `host`, `port` |
| `register_file` | Seeder | Đăng ký metadata file | `file_id`, `file_name`, `file_size`, `chunk_size`, `total_chunks`, `chunks` |
| `announce_chunks` | Peer | Cập nhật chunk peer đã có | `peer_id`, `file_id`, `chunk_indexes` |
| `get_peers` | Leecher | Tìm nguồn cho file/chunk | `file_id`, tùy chọn `chunk_indexes` |
| `heartbeat` | Peer | Gia hạn sự sống | `peer_id` |
| `unregister_peer` | Peer | Rời hệ thống bình thường | `peer_id` |
| `list_files` | Peer | Xem file đang có nguồn online | không bắt buộc |

## Ví dụ

```json
{"type":"get_peers","request_id":"3f3c...","file_id":"a1b2...","chunk_indexes":[0,1,2]}
```

```json
{"request_id":"3f3c...","ok":true,"file_id":"a1b2...","peers":[{"peer_id":"seeder-a","host":"127.0.0.1","port":7001,"chunk_indexes":[0,1,2]}]}
```

## Giao tiếp Peer-to-Peer

Leecher gửi:

```json
{"type":"get_chunk","request_id":"4e4d...","file_id":"a1b2...","chunk_index":2}
```

Seeder thành công trả header:

```json
{"request_id":"4e4d...","ok":true,"file_id":"a1b2...","chunk_index":2,"data_length":1048576}
```

Seeder lập tức gửi tiếp `1048576` bytes chunk. Nếu không có chunk hoặc đang từ chối theo upload policy, Seeder trả JSON lỗi và không gửi bytes.

## Error code chuẩn

`INVALID_REQUEST`, `PEER_NOT_FOUND`, `FILE_NOT_FOUND`, `CHUNK_NOT_FOUND`, `CHUNK_UNAVAILABLE`, `RATE_LIMITED`, `INTERNAL_ERROR`.
