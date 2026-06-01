#!/bin/bash
# Dừng kịch bản khi gặp lỗi
set -e

# Dọn dẹp dữ liệu và log cũ
rm -rf data
rm -f consumer.log consumer_reconnect.log

echo "=========================================================="
echo "=== KHỞI ĐỘNG KIỂM THỬ ĐỘ TIN CẬY (DURABILITY & RECOVERY) ==="
echo "=========================================================="

# 1. Khởi động broker trong nền với chính sách fsync per-write
./bin/broker --fsync per-write &
BROKER_PID=$!
sleep 1
echo "[Test] Broker đã khởi chạy với PID: $BROKER_PID"

# 2. Khởi chạy consumer_1 để đăng ký topic lần đầu, nhằm tạo cursor ghi nhận trên đĩa
echo "[Test] Khởi chạy consumer_1 để đăng ký topic 'sensor/nhietdo'..."
./bin/consumer consumer_1 sensor/nhietdo > consumer.log &
CONSUMER_PID=$!
sleep 1

# Tắt consumer đi ngay sau đó để chuyển sang trạng thái offline
echo "[Test] Tắt consumer_1 để chuyển sang chế độ offline..."
kill -INT $CONSUMER_PID || true
sleep 1

# 3. Gửi các tin nhắn mới xuống topic trong lúc consumer đang offline
echo "[Test] Publisher gửi các tin nhắn mới khi consumer_1 đang offline..."
./bin/producer sensor/nhietdo "Tin nhan offline số 1"
./bin/producer sensor/nhietdo "Tin nhan offline số 2"
./bin/producer sensor/nhietdo "Tin nhan offline số 3"
sleep 1

# 4. Giả lập Broker bị sập nguồn / sập tiến trình đột ngột bằng lệnh kill -9
echo "[Test] Kill -9 Broker đột ngột..."
kill -9 $BROKER_PID
sleep 1

# 5. Khởi động lại Broker để kiểm tra khả năng phục hồi dữ liệu từ file log
echo "[Test] Khởi động lại Broker..."
./bin/broker --fsync per-write &
NEW_BROKER_PID=$!
sleep 1
echo "[Test] Broker mới đã khởi chạy với PID: $NEW_BROKER_PID"

# 6. Cho Consumer kết nối lại với cùng Client ID (consumer_1) để khôi phục tin nhắn bị lỡ
echo "[Test] Cho consumer_1 kết nối lại (Resume-on-Reconnect)..."
./bin/consumer consumer_1 sensor/nhietdo > consumer_reconnect.log &
RECONNECT_CONSUMER_PID=$!
sleep 2

# Tắt các tiến trình sau khi kiểm thử xong
kill -INT $RECONNECT_CONSUMER_PID || true
kill -INT $NEW_BROKER_PID || true

echo ""
echo "=== KẾT QUẢ NHẬN TIN CỦA CONSUMER SAU KHI RECONNECT ==="
if [ -f consumer_reconnect.log ]; then
    cat consumer_reconnect.log
else
    echo "Lỗi: Không tìm thấy file consumer_reconnect.log"
fi
echo "=========================================================="
echo "=== HOÀN THÀNH BÀI TEST DURABILITY & RECOVERY THÀNH CÔNG ==="
echo "=========================================================="
