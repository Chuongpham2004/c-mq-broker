# 🔄 Luồng hoạt động của C-MQ Broker (System Workflow)

Tài liệu này mô tả chi tiết vòng đời của một bản tin và cách các module trong hệ thống C-MQ Broker phối hợp với nhau theo thời gian thực.

---

## 🟢 Giai đoạn 1: Khởi động Server (Broker Startup)
*Quá trình này diễn ra tại `src/server.c` khi chạy lệnh `./bin/broker`.*

1. **Khởi tạo dữ liệu cốt lõi (`init_broker`):** - Đi vào `src/broker.c`, khởi tạo mảng `topics` trống và tạo khóa `global_lock` (Mutex).
   - Gọi `init_storage()` (trong `src/storage.c`) để tạo thư mục `data/` chứa file log lưu trữ `mmap`.
2. **Mở cổng mạng (`create_server_socket`):**
   - Gọi hàm trong `src/network.c` để cấp phát TCP Socket, `bind` vào port 8080 và `listen()`.
3. **Khởi tạo Thread Pool (`threadpool_create`):**
   - Tạo ra một Hàng đợi kết nối (Queue) và sinh ra các Worker threads (ví dụ: 4 threads).
   - Các Worker lập tức gọi `pthread_cond_wait` để "đi ngủ", tránh tiêu hao CPU khi chưa có request.
4. **Boss Thread Trực ban:**
   - Luồng chính (hàm `main`) đi vào vòng lặp `while(keep_running)` và đứng chờ tại lệnh `accept()`.

---

## 🎧 Giai đoạn 2: Consumer Đăng ký (Subscribe)
*Quá trình này xảy ra khi chạy `./bin/consumer`.*

1. **Kết nối:** Consumer gọi `connect()` tới Broker (Port 8080).
2. **Phân phối việc (Boss -> Worker):**
   - Lệnh `accept()` ở Boss Thread bắt được kết nối, tạo ra `client_socket`.
   - Boss gọi `threadpool_add()` để đẩy socket này vào Queue, đồng thời bắn tín hiệu (`pthread_cond_signal`) đánh thức 1 Worker.
3. **Xử lý Giao thức (`handle_client`):**
   - Worker lấy socket ra, gọi `receive_message()` để mút đủ 6 bytes Header (Binary Protocol) và lấy tên Topic.
4. **Ghi danh vào Topic (`broker_subscribe`):**
   - Phát hiện gói tin `MSG_SUBSCRIBE` (ví dụ topic: `sensor/nhietdo`).
   - Khóa Mutex cục bộ của Topic (`topic_lock`), lưu `client_socket` vào mảng `subscriber_sockets`, sau đó mở khóa.
   - Consumer lúc này đi vào trạng thái treo (blocking) để liên tục chờ tin nhắn mới.

---

## 🚀 Giai đoạn 3: Producer Bắn tin & Lưu trữ (Publish & Persist)
*Quá trình này xảy ra khi chạy `./bin/producer`.*

1. **Kết nối & Gửi tin:** - Producer kết nối thành công, Boss Thread lại gọi một Worker khác dậy làm việc.
   - Producer gửi luồng byte chứa `MSG_PUBLISH`, Topic (`sensor/nhietdo`), và Payload (`32.5 do C`).
2. **Ghi đĩa an toàn (`storage_save_message`):**
   - **(Core Feature)** Ngay khi nhận tin, Worker đi vào `src/storage.c`.
   - Tính toán kích thước, gọi `ftruncate` mở rộng file `sensor_nhietdo.log`.
   - Gọi `mmap()` để ánh xạ file lên RAM, ghi bản tin nhị phân vào, và ép lưu xuống đĩa cứng bằng `msync()`.
3. **Định tuyến (Routing - `broker_publish`):**
   - Khóa `topic_lock` của "sensor/nhietdo" lại để tránh Race Condition.
   - Lướt qua mảng `subscriber_sockets`. Tìm thấy socket của Consumer (từ Giai đoạn 2).
   - Gọi lệnh `send()` đẩy nguyên vẹn gói tin (Header + Topic + Payload) qua mạng tới Consumer đó.
   - Mở khóa Mutex. Producer ngắt kết nối an toàn.

---

## 📩 Giai đoạn 4: Consumer Nhận dữ liệu
1. **Phản hồi tức thì:** - Hàm `receive_message()` bên Consumer (đang bị block ở Giai đoạn 2) lập tức quét được dữ liệu nhị phân tràn về.
2. **Dịch ngược & Hiển thị:** - Phân tích byte order (`ntohl`), in ra màn hình: `[NHẬN TIN] Topic: sensor/nhietdo -> Nội dung: 32.5 do C`.
   - Consumer dọn dẹp bộ nhớ đệm và tiếp tục nằm chờ luồng dữ liệu tiếp theo.

---

## 🛑 Giai đoạn 5: Graceful Shutdown (Tắt Server an toàn)
1. **Bắt tín hiệu Hệ thống:** - Khi nhấn `Ctrl + C`, hệ điều hành ném ra tín hiệu `SIGINT`.
2. **Ngắt vòng lặp:** - Hàm `handle_signal` bắt được tín hiệu, đổi cờ `keep_running = 0`.
   - Vòng lặp `accept()` bị ngắt bằng lỗi `EINTR`.
3. **Dọn dẹp:** - Broker đóng `server_socket` chính, từ chối mọi kết nối mới. Các file đã được `munmap()` an toàn ở Giai đoạn 3 không bị rò rỉ (Memory Leak). Hệ thống tắt hoàn toàn.