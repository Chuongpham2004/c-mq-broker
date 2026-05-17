# 🚀 C-MQ Broker (Message Queue System)

Đây là đồ án cuối kỳ môn Lập trình Hệ thống (System Programming) - phát triển một hệ thống Message Broker mô phỏng lại lõi của RabbitMQ/Redis. Hệ thống được viết hoàn toàn bằng Native C trên môi trường Linux.

## 🎯 Tổng quan & Yêu cầu kỹ thuật
Project áp dụng các kỹ năng System Programming cốt lõi để xây dựng một kiến trúc Hybrid Concurrent App:
* **Sockets (TCP/IP):** Quản lý kết nối từ nhiều Publisher và Subscriber cùng lúc.
* **Threads (pthreads):** Sử dụng mô hình Thread Pool (Boss-Worker) để xử lý đa luồng.
* **Synchronization:** Ứng dụng Mutex và Condition Variables để giải quyết bài toán Race Condition, ngăn chặn Deadlock khi đọc/ghi vào message queues.
* **mmap:** Ánh xạ file hệ thống vào bộ nhớ ảo (virtual memory) để lưu trữ (persist) tin nhắn xuống đĩa cứng với tốc độ cao.
* **Signals:** Bắt và xử lý các tín hiệu hệ thống (`SIGINT`, `SIGTERM`) để đóng kết nối và dọn dẹp bộ nhớ an toàn (Graceful Shutdown).

## 🏗️ Kiến trúc Hệ thống

Hệ thống hoạt động theo mô hình **Publish-Subscribe**.
1. **Producer (Người gửi):** Kết nối qua TCP Socket và đẩy tin nhắn vào một `Topic` cụ thể.
2. **Broker (Server trung tâm):** - Nhận request thông qua luồng chính (Boss Thread).
   - Đưa request vào Connection Queue để các Worker Threads trong Pool lấy ra xử lý.
   - Ghi tin nhắn thẳng xuống đĩa thông qua `mmap`.
3. **Consumer (Người nhận):** Đăng ký (Subscribe) vào một `Topic`. Khi có tin nhắn mới ở Topic đó, Broker sẽ lập tức gửi dữ liệu qua TCP Socket cho Consumer.

## 📦 Cấu trúc Giao thức (Binary Protocol)

Để đảm bảo hiệu năng và dễ dàng phân tích gói tin, hệ thống sử dụng **Binary Protocol** tự định nghĩa. Mọi gói tin gửi qua lại đều bắt đầu bằng một **Header cố định dài 6 bytes**:

| Trường dữ liệu | Kích thước | Mô tả |
| :--- | :--- | :--- |
| `type` | 1 byte | Loại lệnh: `1` (CONNECT), `2` (PUBLISH), `3` (SUBSCRIBE), `4` (ACK) |
| `topic_len` | 1 byte | Độ dài của tên Topic (Tối đa 255 ký tự) |
| `payload_len` | 4 bytes | Độ dài của nội dung tin nhắn (Tối đa ~4GB) |

*Ngay sau 6 bytes Header này sẽ là chuỗi bytes chứa tên Topic, và tiếp theo là chuỗi bytes chứa Payload (nội dung).*

## 📂 Cấu trúc Thư mục

```text
c-mq-broker/
├── Makefile            # Chứa các lệnh build tự động
├── README.md           # Tài liệu dự án
├── include/            # Thư mục chứa các file .h (Định nghĩa struct, tên hàm)
│   ├── network.h       # Khai báo cấu hình TCP Server
│   ├── threadpool.h    # Khai báo Thread Pool & Queue
│   ├── broker.h        # Khai báo logic Pub/Sub & Mutex
│   └── protocol.h      # Khai báo cấu trúc Giao thức (Header)
├── src/                # Thư mục chứa source code chính .c
│   ├── server.c        # Hàm main(), khởi chạy Broker
│   ├── network.c       # Xử lý tạo socket, bind, listen, accept
│   ├── threadpool.c    # Quản lý vòng đời của threads
│   ├── broker.c        # Xử lý logic định tuyến bản tin
│   └── storage.c       # Xử lý mmap lưu file hệ thống
└── clients/            # Thư mục chứa code giả lập Client để test
    ├── producer.c      
    └── consumer.c


🛠️ Hướng dẫn Cài đặt & Chạy
(Lưu ý: Phải chạy trên môi trường Linux hoặc WSL do sử dụng các thư viện POSIX).

1. Biên dịch dự án:

Bash
# Lệnh make sẽ tự động tìm và biên dịch các file .c trong thư mục src/
make all
2. Khởi chạy Server (Broker):

Bash
./bin/broker
3. Khởi chạy Client test:
Mở các terminal khác nhau để chạy giả lập Producer và Consumer:

Bash
./bin/consumer
./bin/producer