# Demo tuần 2

PostgreSQL local và .env chuẩn bị theo WEEK1_DEMO.md. Chạy các lệnh từ thư
mục gốc trong các terminal riêng. Lấy file_id từ log Seeder.

```powershell
python -m tracker.server
```

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-a --port 7001 --chunk-size 64 --workers 4
```

```powershell
python -m peer.uploader samples/README.md --peer-id seeder-b --port 7002 --chunk-size 64 --workers 4
```

```powershell
python -m peer.downloader FILE_ID --workers 2 --output-dir downloads
```

Phải có log chunk OK từ cả seeder-a và seeder-b. File sau ghép nằm ở
downloads/FILE_ID/completed/README.md. Get-FileHash file đó và samples/README.md
phải giống nhau. Thư mục completed ngăn tên file gốc trùng 0.part làm mất chunk.
Không đổi file nguồn trong lúc chạy. Ctrl+C các Seeder trước rồi dừng Tracker.

Test tự động: python -m unittest discover -s tests -v. Các test PostgreSQL
cần TRACKER_TEST_DSN và TRACKER_UPLOADER_SOURCE như WEEK1_DEMO.md. Khi đặt
TRACKER_TEST_LOG_DIR, bài E2E giữ log e2e-week2-full-file.log và week2-result.json
chứng minh nhận 9 chunk/8458 bytes từ hai Seeder, bytes/hash toàn file đúng.
Nguồn tạo cùng metadata bằng cùng file và chunk_size; database test tạm riêng
được tự dọn. Không commit log chứa credential hoặc .env lên Git.
