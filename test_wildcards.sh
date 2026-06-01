#!/bin/bash
set -e

# Dọn dẹp log cũ
rm -rf data
rm -f wildcard_plus.log wildcard_hash.log

echo "=========================================================="
echo "=== KHỞI ĐỘNG KIỂM THỬ KÝ TỰ ĐẠI DIỆN (TOPIC WILDCARDS) ==="
echo "=========================================================="

# 1. Khởi động broker trong nền
./bin/broker &
BROKER_PID=$!
sleep 1

# 2. Khởi chạy consumer đăng ký wildcard '+' (sensor/+/temp)
echo "[Test] Khởi chạy consumer_plus đăng ký 'sensor/+/temp'..."
./bin/consumer consumer_plus "sensor/+/temp" > wildcard_plus.log &
PLUS_PID=$!

# 3. Khởi chạy consumer đăng ký wildcard '#' (sensor/#)
echo "[Test] Khởi chạy consumer_hash đăng ký 'sensor/#'..."
./bin/consumer consumer_hash "sensor/#" > wildcard_hash.log &
HASH_PID=$!
sleep 1

# 4. Gửi các tin nhắn tới các topic khác nhau
echo "[Test] Publisher gửi tin nhắn tới 'sensor/room1/temp'..."
./bin/producer "sensor/room1/temp" "Nhiet do room1 la 25"
sleep 1

echo "[Test] Publisher gửi tin nhắn tới 'sensor/room1/room2/temp'..."
./bin/producer "sensor/room1/room2/temp" "Nhiet do room2 la 30"
sleep 1

echo "[Test] Publisher gửi tin nhắn tới 'other/topic'..."
./bin/producer "other/topic" "Tin nhan khac"
sleep 1

# Tắt các consumer và broker
kill -INT $PLUS_PID || true
kill -INT $HASH_PID || true
kill -INT $BROKER_PID || true
sleep 1

echo ""
echo "=== KẾT QUẢ CỦA CONSUMER PLUS (sensor/+/temp) ==="
echo "Mong đợi: Chỉ nhận được tin nhắn từ 'sensor/room1/temp'"
echo "----------------------------------------------------------"
cat wildcard_plus.log | grep "\[NHẬN TIN\]" || echo "(Không nhận được tin nhắn)"
echo "----------------------------------------------------------"

echo ""
echo "=== KẾT QUẢ CỦA CONSUMER HASH (sensor/#) ==="
echo "Mong đợi: Nhận được tin nhắn từ 'sensor/room1/temp' và 'sensor/room1/room2/temp'"
echo "----------------------------------------------------------"
cat wildcard_hash.log | grep "\[NHẬN TIN\]" || echo "(Không nhận được tin nhắn)"
echo "----------------------------------------------------------"

echo "=========================================================="
echo "=== HOÀN THÀNH BÀI TEST TOPIC WILDCARDS THÀNH CÔNG ==="
echo "=========================================================="
