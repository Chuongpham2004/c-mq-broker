# 🚀 C-MQ Broker (System Programming Final Project)

Dự án cuối kỳ môn **Lập trình Hệ thống (System Programming)** - Xây dựng hệ thống Message Queue / Broker hiệu năng cao mô phỏng lại các lõi RabbitMQ/Redis bằng ngôn ngữ Native C (chuẩn C11) trên môi trường POSIX/Linux.

---

## 👥 Thông tin Nhóm & Thành viên

* **Tên môn học:** Lập trình Hệ thống (System Programming)
* **Nhóm sinh viên thực hiện:**
  1. **Nguyễn Văn A** (MSSV: 22010001) - Email: A.nguyen@vnu.edu.vn
  2. **Trần Thị B** (MSSV: 22010002) - Email: B.tran@vnu.edu.vn
  3. **Lê Văn C** (MSSV: 22010003) - Email: C.le@vnu.edu.vn

---

## 🎯 Tính năng Hệ thống

Hệ thống được phát triển tuân thủ kiến trúc đa luồng đồng thời (**Hybrid Concurrent Architecture**):
* **Sockets (TCP/IP):** Quản lý kết nối non-blocking và xử lý đồng thời từ nhiều Publisher/Subscriber.
* **Thread Pool:** Mô hình Boss-Worker với 4 luồng xử lý đồng thời, quản lý hàng đợi kết nối an toàn.
* **Topic Wildcards Routing:** Định tuyến tin nhắn theo cấp độ hỗ trợ dấu cộng `+` (khớp đúng 1 cấp độ) và dấu thăng `#` (khớp nhiều cấp độ).
* **Storage & MSync:** Ánh xạ tệp bằng `mmap` ghi nối tiếp (append-only) tin nhắn và hỗ trợ ba chính sách đồng bộ ổ đĩa: `per-write`, `group-commit` (gom 5 tin nhắn), `time-based` (1 giây).
* **Reliability (Độ tin cậy):** Đảm bảo không mất tin nhắn với **Pending ACK** và cơ chế tự động gửi lại (**Redelivery**) tối đa 3 lần sau mỗi 5 giây.
* **Session Resuming (Khôi phục trạng thái):** Lưu trữ con trỏ đọc (**Cursor Offset**) của mỗi Consumer xuống đĩa cứng (`data/cursors.txt`). Khi Consumer kết nối lại, hệ thống sẽ khôi phục session và gửi lại toàn bộ tin nhắn bị lỡ (**Resume-on-reconnect**).
* **Retention Policy:** Cơ chế giải phóng dung lượng đĩa tự động dựa trên thời gian (giây), dung lượng tối đa (bytes), hoặc khi tất cả Consumer đã ACK.
* **Graceful Shutdown:** Bắt tín hiệu ngắt (`SIGINT`, `SIGTERM`) để giải phóng toàn bộ vùng nhớ `mmap`, dọn dẹp kết nối, và hủy đăng ký (`MSG_UNSUBSCRIBE`) trước khi thoát.

---

## 📦 Cấu trúc Giao thức (Binary Protocol)

Mọi gói tin truyền qua mạng đều có **Header cố định dài 14 bytes** để phân tích nhị phân tốc độ cao:

| Trường dữ liệu | Kích thước | Mô tả |
| :--- | :--- | :--- |
| `type` | 1 byte | Loại lệnh: `1` (CONNECT), `2` (PUBLISH), `3` (SUBSCRIBE), `4` (ACK), `5` (UNSUBSCRIBE) |
| `topic_len` | 1 byte | Độ dài của chuỗi tên Topic (Tối đa 255 ký tự) |
| `msg_id` | 4 bytes | ID duy nhất của tin nhắn dùng để khớp ACK và Redelivery |
| `timestamp` | 4 bytes | Nhãn thời gian Epoch phục vụ chính sách Retention |
| `payload_len` | 4 bytes | Độ dài nội dung dữ liệu (Payload) |

---

## 📂 Cấu trúc Thư mục

```text
c-mq-broker/
├── Makefile            # Chứa các luật build và rule chạy test
├── README.md           # Tài liệu dự án và hướng dẫn sử dụng
├── test_suite.sh       # Script kiểm thử tự động hóa 5 test cases
├── include/            # Định nghĩa các thư viện tiêu đề (.h)
│   ├── network.h       
│   ├── threadpool.h    
│   ├── broker.h        
│   └── protocol.h      
├── src/                # Triển khai source code chính (.c)
│   ├── server.c        # Khởi tạo và phân tích tham số dòng lệnh
│   ├── network.c       # Giao tiếp socket
│   ├── threadpool.c    # Quản lý luồng xử lý
│   ├── broker.c        # Logic định tuyến wildcard, ACK, cursor và retention
│   └── storage.c       # Cơ chế mmap, msync và trim_log_file
└── clients/            # Mã nguồn các Client để mô phỏng và test
    ├── producer.c      # Client gửi tin nhắn
    ├── consumer.c      # Client nhận tin nhắn có auto-ACK và unsubscribe
    └── noack_client.c  # Client nhận tin không gửi ACK (để test lỗi/timeout)
```

---

## 🛠️ Hướng dẫn Biên dịch & Chạy

### 1. Biên dịch Dự án
Sử dụng Makefile biên dịch sạch với các tùy chọn C11 (`-std=c11`, `-Wall`, `-Wextra`):
```bash
make clean
make all
```

### 2. Sử dụng Broker (Server)
Khởi chạy Server với các tùy chọn cấu hình tham số dòng lệnh tùy ý:
```bash
# Cú pháp
./bin/broker [--fsync <per-write|group-commit|time-based>] [--retention-time <seconds>] [--retention-size <bytes>] [--retention-acked]

# Ví dụ chạy với chính sách gom tin nhắn và dọn dẹp các tin nhắn cũ hơn 10 phút (600 giây)
./bin/broker --fsync group-commit --retention-time 600
```

### 3. Sử dụng Consumer & Producer
* **Mở Consumer lắng nghe:**
  ```bash
  # Cú pháp: ./bin/consumer <client_id> <topic_pattern>
  # Đăng ký nhận tin nhắn với client ID 'client1' trên topic wildcard
  ./bin/consumer client1 "sensor/+/temp"
  ```

* **Mở Producer gửi dữ liệu:**
  ```bash
  # Cú pháp: ./bin/producer <topic> <payload>
  ./bin/producer "sensor/room1/temp" "28.5 degree"
  ```

---

## 🧪 Quy trình Kiểm thử (Testing)

Dự án bao gồm một hệ thống kiểm thử tích hợp tự động hóa qua 5 kịch bản chính. Chỉ cần chạy một lệnh duy nhất:
```bash
make test
```

### Chi tiết 5 kịch bản kiểm thử:
1. **Test Case 1: Connect, Subscribe, Publish, & Receive (Normal Case)**
   * Đăng ký một consumer, gửi tin nhắn cơ bản từ producer và kiểm tra xem consumer có nhận được chính xác nội dung dữ liệu qua socket mạng hay không.
2. **Test Case 2: Topic Wildcards (Normal Case)**
   * Kiểm tra định tuyến wildcard mức độ phức tạp với dấu `+` và `#`. Đảm bảo topic `sensor/+/temp` nhận đúng `sensor/room1/temp` (không nhận nhiều cấp), còn `sensor/#` nhận toàn bộ các cấp con.
3. **Test Case 3: Offline Queueing & Session Resuming (Edge Case)**
   * Consumer đăng ký và ngắt kết nối. Producer gửi tin nhắn khi consumer offline. Khởi động lại Broker đột ngột (`kill -9`) và cho consumer kết nối lại. Xác minh consumer nhận lại đầy đủ tin nhắn bị lỡ thông qua con trỏ đĩa đệm.
4. **Test Case 4: Clean Unsubscribe (Edge Case)**
   * Consumer subscribe, nhận tin nhắn 1, gửi lệnh `MSG_UNSUBSCRIBE` và đóng kết nối. Producer gửi tiếp tin nhắn 2. Đảm bảo tin nhắn 2 không được đẩy vào cursor của client này sau khi đã hủy đăng ký.
5. **Test Case 5: Pending ACK & Redelivery/Timeout (Error Case)**
   * Kết nối client đặc biệt không gửi ACK. Gửi tin nhắn, kiểm tra xem Broker có tự động gửi lại tin nhắn 3 lần sau mỗi 5 giây hay không, và đảm bảo Broker chủ động ngắt kết nối socket của client vi phạm sau khi quá hạn retry.

---

## ✍️ Phân chia Đóng góp Thành viên (Contributions)

* **Nguyễn Văn A (Nhóm trưởng):** Thiết lập kiến trúc mạng đa luồng, viết Thread Pool và Socket Handler. Triển khai cấu trúc gói tin Binary Protocol dài 14 bytes và cơ chế đóng gói mạng, bảo đảm biên dịch sạch chuẩn C11 trên GCC Linux.
* **Trần Thị B:** Triển khai tầng lưu trữ đĩa đệm `storage.c`, cấu trúc lưu trữ append-only log bằng `mmap` kết hợp `msync`. Thiết lập các chính sách đồng bộ ổ đĩa (`per-write`, `group-commit`, `time-based`) và cơ chế dọn dẹp log bằng hàm `trim_log_file`.
* **Lê Văn C:** Triển khai các kịch bản định tuyến Wildcard (`match_topic`), cơ chế Pending ACK / Redelivery và quản lý cursor lưu trữ session khôi phục kết nối. Chịu trách nhiệm viết script `test_suite.sh` và thiết kế kịch bản test 5 trường hợp nâng cao.