#!/bin/bash
# Dừng kịch bản khi gặp lỗi
set -e

# Khai báo màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Dọn dẹp dữ liệu cũ và tiến trình chạy ngầm trước khi test
killall broker consumer noack_client 2>/dev/null || true
sleep 1
rm -rf data
rm -f tc1.log tc2_plus.log tc2_hash.log tc3.log tc4.log tc5_broker.log

echo "=========================================================="
echo "=== BẮT ĐẦU CHẠY HỆ THỐNG KIỂM THỬ (5 TEST CASES) ==="
echo "=========================================================="

# Khởi chạy broker trong nền, ghi nhận log của broker vào file tc5_broker.log
./bin/broker --fsync per-write > tc5_broker.log 2>&1 &
BROKER_PID=$!
sleep 1

# ==========================================================
# TEST CASE 1: Đăng ký, Gửi nhận tin nhắn cơ bản (Normal Case)
# ==========================================================
echo -e "\n[Test Case 1] Đang kiểm tra gửi nhận tin nhắn cơ bản..."
./bin/consumer tc1_consumer sensor/temp > tc1.log 2>&1 &
TC1_CONSUMER_PID=$!
sleep 1

./bin/producer sensor/temp "Hello World" > /dev/null
sleep 1

kill -INT $TC1_CONSUMER_PID || true
wait $TC1_CONSUMER_PID 2>/dev/null || true

if grep -q "Hello World" tc1.log; then
    echo -e "${GREEN}[TC1 PASS]${NC} Gửi nhận cơ bản thành công!"
else
    echo -e "${RED}[TC1 FAIL]${NC} Không nhận được tin nhắn gửi cơ bản."
fi

# ==========================================================
# TEST CASE 2: Kiểm tra ký tự đại diện Wildcards (Normal Case)
# ==========================================================
echo -e "\n[Test Case 2] Đang kiểm tra Wildcard topic (+ và #)..."
./bin/consumer tc2_plus "tc2/+/temp" > tc2_plus.log 2>&1 &
PLUS_PID=$!
./bin/consumer tc2_hash "tc2/#" > tc2_hash.log 2>&1 &
HASH_PID=$!
sleep 1

./bin/producer "tc2/room1/temp" "Room1 Temp is 25" > /dev/null
./bin/producer "tc2/room1/room2/temp" "Room2 Temp is 30" > /dev/null
sleep 1

kill -INT $PLUS_PID || true
kill -INT $HASH_PID || true
wait $PLUS_PID $HASH_PID 2>/dev/null || true

# PLUS mong đợi nhận được room1, nhưng KHÔNG nhận được room2
# HASH mong đợi nhận được cả hai
PLUS_OK=0
HASH_OK=0

if grep -q "Room1 Temp is 25" tc2_plus.log && ! grep -q "Room2 Temp is 30" tc2_plus.log; then
    PLUS_OK=1
fi

if grep -q "Room1 Temp is 25" tc2_hash.log && grep -q "Room2 Temp is 30" tc2_hash.log; then
    HASH_OK=1
fi

if [ $PLUS_OK -eq 1 ] && [ $HASH_OK -eq 1 ]; then
    echo -e "${GREEN}[TC2 PASS]${NC} Các quy tắc Wildcard (+ và #) hoạt động chính xác!"
else
    echo -e "${RED}[TC2 FAIL]${NC} Lỗi định tuyến wildcard (+ OK: $PLUS_OK, # OK: $HASH_OK)."
fi

# ==========================================================
# TEST CASE 3: Offline Queueing & Session Resuming (Edge Case)
# ==========================================================
echo -e "\n[Test Case 3] Đang kiểm tra Offline Queueing & Session Resuming..."
# Khởi chạy để tạo session
./bin/consumer tc3_consumer tc3/topic >> tc3.log 2>&1 &
TC3_CONSUMER_PID=$!
sleep 1

# Tắt đi
kill -INT $TC3_CONSUMER_PID || true
wait $TC3_CONSUMER_PID 2>/dev/null || true
sleep 1

# Publish tin nhắn khi offline
./bin/producer tc3/topic "Tin nhan offline 1" > /dev/null
./bin/producer tc3/topic "Tin nhan offline 2" > /dev/null
sleep 1

# Khởi chạy lại với cùng client_id
./bin/consumer tc3_consumer tc3/topic >> tc3.log 2>&1 &
TC3_NEW_CONSUMER_PID=$!
sleep 1.5

kill -INT $TC3_NEW_CONSUMER_PID || true
wait $TC3_NEW_CONSUMER_PID 2>/dev/null || true

if grep -q "Tin nhan offline 1" tc3.log && grep -q "Tin nhan offline 2" tc3.log; then
    echo -e "${GREEN}[TC3 PASS]${NC} Offline Queueing & Session Resuming thành công!"
else
    echo -e "${RED}[TC3 FAIL]${NC} Không phục hồi được các tin nhắn cũ bị nhỡ."
fi

# ==========================================================
# TEST CASE 4: Hủy đăng ký Clean Unsubscribe (Edge Case)
# ==========================================================
echo -e "\n[Test Case 4] Đang kiểm tra Clean Unsubscribe..."
./bin/consumer tc4_consumer tc4/topic > tc4.log 2>&1 &
TC4_CONSUMER_PID=$!
sleep 1

./bin/producer tc4/topic "Tin nhan truoc khi unsub" > /dev/null
sleep 0.5

# Gửi tín hiệu INT (SIGINT) đến consumer để nó thực hiện gửi yêu cầu UNSUBSCRIBE rồi tắt
kill -INT $TC4_CONSUMER_PID || true
wait $TC4_CONSUMER_PID 2>/dev/null || true
sleep 1

# Gửi tiếp tin nhắn sau khi đã Unsubscribe
./bin/producer tc4/topic "Tin nhan sau khi unsub" > /dev/null
sleep 1

# Check log: Phải nhận được tin nhắn 1, nhưng KHÔNG nhận được tin nhắn 2
if grep -q "Tin nhan truoc khi unsub" tc4.log && ! grep -q "Tin nhan sau khi unsub" tc4.log; then
    echo -e "${GREEN}[TC4 PASS]${NC} Clean Unsubscribe hoạt động chính xác!"
else
    echo -e "${RED}[TC4 FAIL]${NC} Hủy đăng ký không thành công hoặc nhận sai tin nhắn."
fi

# ==========================================================
# TEST CASE 5: Pending ACK & Redelivery/Timeout (Error Case)
# ==========================================================
echo -e "\n[Test Case 5] Đang kiểm tra Pending ACK & Redelivery (Timeout)..."
# Khởi chạy client đặc biệt nhận tin nhắn nhưng cố ý KHÔNG gửi ACK phản hồi
./bin/noack_client > /dev/null 2>&1 &
NOACK_PID=$!
sleep 1

# Publish tin nhắn để kích hoạt redelivery
./bin/producer sensor/noack "Tin nhan cho NoACK" > /dev/null
sleep 22 # Chờ Broker thử gửi lại (5s mỗi lần, tối đa 3 lần là 15s, lần thứ 4 ở giây thứ 20 sẽ ngắt kết nối)

# Dừng noack client
kill -INT $NOACK_PID || true
wait $NOACK_PID 2>/dev/null || true

# Tắt broker hoàn toàn
kill -INT $BROKER_PID || true
wait $BROKER_PID 2>/dev/null || true

# Đọc log broker để xác minh broker đã gửi lại tin nhắn và đóng kết nối do timeout
REDELIVER_COUNT=$(grep -c "Gửi lại tin nhắn ID" tc5_broker.log || true)
DISCONNECT_OK=$(grep -c "sau 3 lần" tc5_broker.log || true)

if [ $REDELIVER_COUNT -ge 3 ] && [ $DISCONNECT_OK -ge 1 ]; then
    echo -e "${GREEN}[TC5 PASS]${NC} Broker đã gửi lại tin nhắn ($REDELIVER_COUNT lần) và ngắt kết nối chính xác!"
else
    echo -e "${RED}[TC5 FAIL]${NC} Cơ chế Redelivery/Timeout không đúng (Gửi lại: $REDELIVER_COUNT, Ngắt kết nối: $DISCONNECT_OK)."
fi

echo -e "\n=========================================================="
echo "=== HOÀN THÀNH TOÀN BỘ HỆ THỐNG KIỂM THỬ ==="
echo "=========================================================="
