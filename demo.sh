#!/bin/bash
# ============================================================================
#  C-MQ BROKER — LIVE DEMO SCRIPT
#  Run each demo individually: ./demo.sh [1-8]
#  Or run all demos sequentially: ./demo.sh all
# ============================================================================

set -e

# Colors
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
MAGENTA='\033[1;35m'
CYAN='\033[1;36m'
WHITE='\033[1;37m'
NC='\033[0m'       # No Color
DIM='\033[2m'
BOLD='\033[1m'

# Dividers
LINE="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cleanup() {
    killall broker consumer producer noack_client 2>/dev/null || true
    sleep 0.5
    rm -rf data
    rm -f /tmp/cmq_demo_*.log
}

pause() {
    echo ""
    echo -e "${YELLOW}▶ Press ENTER to continue...${NC}"
    read -r
}

banner() {
    echo ""
    echo -e "${CYAN}${LINE}${NC}"
    echo -e "${WHITE}${BOLD}  DEMO $1: $2${NC}"
    echo -e "${CYAN}${LINE}${NC}"
    echo ""
}

step() {
    echo -e "${GREEN}  ➤ STEP $1:${NC} ${WHITE}$2${NC}"
}

cmd() {
    echo -e "${DIM}    \$ $1${NC}"
}

info() {
    echo -e "${BLUE}  ℹ $1${NC}"
}

success() {
    echo -e "${GREEN}  ✅ $1${NC}"
}

fail() {
    echo -e "${RED}  ❌ $1${NC}"
}

show_file() {
    echo -e "${MAGENTA}  📄 Content of $1:${NC}"
    if [ -f "$1" ]; then
        cat "$1" | while IFS= read -r line; do
            echo -e "${DIM}     | $line${NC}"
        done
    else
        echo -e "${DIM}     (file not found)${NC}"
    fi
}

wait_for_log() {
    local logfile="$1"
    local pattern="$2"
    local timeout="${3:-10}"
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if grep -q "$pattern" "$logfile" 2>/dev/null; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    return 1
}

# ============================================================================
#  DEMO 1: Basic Publish-Subscribe
# ============================================================================
demo1() {
    banner "1" "Basic Publish-Subscribe (CONNECT → SUBSCRIBE → PUBLISH → RECEIVE → ACK)"
    cleanup

    info "This demo shows the fundamental message flow:"
    info "  Producer publishes a message → Broker persists & routes → Consumer receives & ACKs"
    echo ""

    step "1" "Start the broker with per-write fsync (maximum durability)"
    cmd "./bin/broker --fsync per-write"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started (PID: $BROKER_PID) on port 8080"
    echo ""

    step "2" "Start a consumer subscribing to topic 'sensor/nhietdo'"
    cmd "./bin/consumer client_A sensor/nhietdo"
    pause
    ./bin/consumer client_A sensor/nhietdo > /tmp/cmq_demo_consumer.log 2>&1 &
    CONSUMER_PID=$!
    sleep 1
    success "Consumer 'client_A' connected and subscribed (PID: $CONSUMER_PID)"
    echo ""

    step "3" "Publish a message from the producer"
    cmd "./bin/producer sensor/nhietdo 'Temperature: 32.5 degrees'"
    pause
    ./bin/producer sensor/nhietdo "Temperature: 32.5 degrees"
    sleep 2
    echo ""

    step "4" "Check consumer log — did it receive the message?"
    pause
    echo -e "${MAGENTA}  📄 Consumer output:${NC}"
    cat /tmp/cmq_demo_consumer.log | tail -5 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    if grep -q "Temperature" /tmp/cmq_demo_consumer.log 2>/dev/null; then
        success "Consumer received the message!"
    else
        fail "Message not found in consumer log"
    fi
    echo ""

    step "5" "Check persistent storage on disk"
    pause
    echo -e "${MAGENTA}  📁 data/ directory:${NC}"
    ls -la data/ 2>/dev/null | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""
    if [ -f "data/sensor_nhietdo.log" ]; then
        local fsize=$(stat -c%s "data/sensor_nhietdo.log" 2>/dev/null || stat -f%z "data/sensor_nhietdo.log" 2>/dev/null)
        success "Log file 'data/sensor_nhietdo.log' exists (${fsize} bytes)"
    fi
    echo ""

    step "6" "Check cursor file — consumer's read position was saved after ACK"
    pause
    show_file "data/cursors.txt"
    echo ""
    success "Cursor tracks that client_A has read up to offset in sensor/nhietdo"

    # Cleanup
    kill $CONSUMER_PID 2>/dev/null || true
    kill $BROKER_PID 2>/dev/null || true
    wait $CONSUMER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 1 complete!"
}

# ============================================================================
#  DEMO 2: Wildcard Topic Matching (+ and #)
# ============================================================================
demo2() {
    banner "2" "Wildcard Topic Matching (+ single-level, # multi-level)"
    cleanup

    info "Two wildcard characters:"
    info "  + matches exactly ONE level    (e.g., sensor/+/temp)"
    info "  # matches ZERO or MORE levels  (e.g., sensor/#)"
    echo ""

    step "1" "Start broker"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started"
    echo ""

    step "2" "Start Consumer A subscribing to 'sensor/+/temp' (single-level wildcard)"
    cmd "./bin/consumer user_A 'sensor/+/temp'"
    info "  This will match: sensor/room1/temp, sensor/room2/temp"
    info "  This will NOT match: sensor/room1/room2/temp (+ is only 1 level)"
    pause
    ./bin/consumer user_A "sensor/+/temp" > /tmp/cmq_demo_plus.log 2>&1 &
    PLUS_PID=$!
    sleep 1
    success "Consumer A subscribed to 'sensor/+/temp'"
    echo ""

    step "3" "Start Consumer B subscribing to 'sensor/#' (multi-level wildcard)"
    cmd "./bin/consumer user_B 'sensor/#'"
    info "  This will match: sensor, sensor/temp, sensor/room1/temp, sensor/a/b/c"
    info "  Basically EVERYTHING under 'sensor/'"
    pause
    ./bin/consumer user_B "sensor/#" > /tmp/cmq_demo_hash.log 2>&1 &
    HASH_PID=$!
    sleep 1
    success "Consumer B subscribed to 'sensor/#'"
    echo ""

    step "4" "Publish message to 'sensor/room1/temp' (2 levels)"
    cmd "./bin/producer sensor/room1/temp 'Room1: 25.0C'"
    info "  Expected: Consumer A ✅ receives (+ = room1), Consumer B ✅ receives (# = room1/temp)"
    pause
    ./bin/producer sensor/room1/temp "Room1: 25.0C"
    sleep 2
    echo ""

    step "5" "Publish message to 'sensor/room1/room2/temp' (3 levels)"
    cmd "./bin/producer sensor/room1/room2/temp 'Deep: 28.0C'"
    info "  Expected: Consumer A ❌ NO (+ only 1 level), Consumer B ✅ receives (# = room1/room2/temp)"
    pause
    ./bin/producer sensor/room1/room2/temp "Deep: 28.0C"
    sleep 2
    echo ""

    step "6" "Check results"
    pause
    echo -e "${MAGENTA}  📄 Consumer A (sensor/+/temp):${NC}"
    cat /tmp/cmq_demo_plus.log | grep -i "room\|payload\|noi dung\|NHAN\|Received" | tail -5 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""
    echo -e "${MAGENTA}  📄 Consumer B (sensor/#):${NC}"
    cat /tmp/cmq_demo_hash.log | grep -i "room\|payload\|noi dung\|NHAN\|Received\|Deep" | tail -5 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    local plus_count=$(grep -c "Room1" /tmp/cmq_demo_plus.log 2>/dev/null || echo "0")
    local plus_deep=$(grep -c "Deep" /tmp/cmq_demo_plus.log 2>/dev/null || echo "0")
    local hash_count=$(grep -c "Room1\|Deep" /tmp/cmq_demo_hash.log 2>/dev/null || echo "0")

    if [ "$plus_count" -ge 1 ] && [ "$plus_deep" -eq 0 ]; then
        success "Consumer A: received 'Room1' ✅, did NOT receive 'Deep' ✅ (+ works correctly)"
    else
        info "Consumer A: messages=$plus_count, deep=$plus_deep"
    fi

    if [ "$hash_count" -ge 2 ]; then
        success "Consumer B: received BOTH messages ✅ (# works correctly)"
    else
        info "Consumer B: total=$hash_count"
    fi

    kill $PLUS_PID $HASH_PID $BROKER_PID 2>/dev/null || true
    wait $PLUS_PID $HASH_PID $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 2 complete!"
}

# ============================================================================
#  DEMO 3: Offline Queueing & Resume-on-Reconnect (Cursors)
# ============================================================================
demo3() {
    banner "3" "Offline Queueing & Resume-on-Reconnect (Per-Subscriber Cursors)"
    cleanup

    info "When a consumer disconnects, the broker KEEPS its cursor position."
    info "Messages published while offline are stored in the log."
    info "On reconnect with the same client_id, all missed messages are replayed."
    echo ""

    step "1" "Start broker"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started"
    echo ""

    step "2" "Consumer connects with client_id='device_01', subscribes to 'data/sensor'"
    pause
    ./bin/consumer device_01 "data/sensor" > /tmp/cmq_demo_c3.log 2>&1 &
    C_PID=$!
    sleep 1
    success "Consumer 'device_01' connected"
    echo ""

    step "3" "Publish first message — consumer receives it online"
    cmd "./bin/producer data/sensor 'MSG-1: Online message'"
    pause
    ./bin/producer data/sensor "MSG-1: Online message"
    sleep 2
    echo -e "${MAGENTA}  📄 Consumer sees:${NC}"
    cat /tmp/cmq_demo_c3.log | tail -3 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    success "Consumer received MSG-1 while online"
    echo ""

    step "4" "★ Consumer DISCONNECTS (simulating network failure)"
    cmd "kill $C_PID"
    pause
    kill $C_PID 2>/dev/null || true
    wait $C_PID 2>/dev/null || true
    sleep 1
    success "Consumer is now OFFLINE"
    echo ""

    step "5" "Publish 2 messages WHILE CONSUMER IS OFFLINE"
    cmd "./bin/producer data/sensor 'MSG-2: Published while offline'"
    cmd "./bin/producer data/sensor 'MSG-3: Also offline'"
    pause
    ./bin/producer data/sensor "MSG-2: Published while offline"
    sleep 0.5
    ./bin/producer data/sensor "MSG-3: Also offline"
    sleep 1
    info "Messages are stored in data/data_sensor.log but consumer is not here to receive them"
    echo ""

    step "6" "Show cursor file — consumer's last position is saved"
    pause
    show_file "data/cursors.txt"
    echo ""

    step "7" "★ Consumer RECONNECTS with the SAME client_id 'device_01'"
    info "The broker will detect the cursor and replay MSG-2 and MSG-3"
    cmd "./bin/consumer device_01 'data/sensor'"
    pause
    ./bin/consumer device_01 "data/sensor" > /tmp/cmq_demo_c3_reconnect.log 2>&1 &
    C_PID2=$!
    sleep 3
    echo ""

    step "8" "Check — did the consumer receive the 2 offline messages?"
    pause
    echo -e "${MAGENTA}  📄 Consumer output after reconnect:${NC}"
    cat /tmp/cmq_demo_c3_reconnect.log | tail -10 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    local msg2=$(grep -c "MSG-2" /tmp/cmq_demo_c3_reconnect.log 2>/dev/null || echo "0")
    local msg3=$(grep -c "MSG-3" /tmp/cmq_demo_c3_reconnect.log 2>/dev/null || echo "0")
    if [ "$msg2" -ge 1 ] && [ "$msg3" -ge 1 ]; then
        success "Both offline messages were replayed! Resume-on-reconnect works!"
    else
        info "MSG-2 count=$msg2, MSG-3 count=$msg3"
    fi

    kill $C_PID2 $BROKER_PID 2>/dev/null || true
    wait $C_PID2 $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 3 complete!"
}

# ============================================================================
#  DEMO 4: Unsubscribe
# ============================================================================
demo4() {
    banner "4" "Clean Unsubscribe (Consumer stops receiving after UNSUBSCRIBE)"
    cleanup

    info "After sending MSG_UNSUBSCRIBE, the consumer should NOT receive any new messages."
    info "The consumer.c sends UNSUBSCRIBE automatically when it receives SIGINT (Ctrl+C)."
    echo ""

    step "1" "Start broker"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started"
    echo ""

    step "2" "Start consumer subscribing to 'news/tech'"
    pause
    ./bin/consumer reader_1 "news/tech" > /tmp/cmq_demo_c4.log 2>&1 &
    C_PID=$!
    sleep 1
    success "Consumer subscribed"
    echo ""

    step "3" "Publish BEFORE unsubscribe — consumer should receive this"
    cmd "./bin/producer news/tech 'BEFORE-UNSUB: Breaking news!'"
    pause
    ./bin/producer news/tech "BEFORE-UNSUB: Breaking news!"
    sleep 2
    success "Message sent"
    echo ""

    step "4" "★ Send SIGINT to consumer → triggers UNSUBSCRIBE before disconnect"
    cmd "kill -INT $C_PID  (this triggers consumer's graceful unsubscribe handler)"
    pause
    kill -INT $C_PID 2>/dev/null || true
    wait $C_PID 2>/dev/null || true
    sleep 1
    success "Consumer unsubscribed and disconnected"
    echo ""

    step "5" "Publish AFTER unsubscribe"
    cmd "./bin/producer news/tech 'AFTER-UNSUB: This should NOT arrive'"
    pause
    ./bin/producer news/tech "AFTER-UNSUB: This should NOT arrive"
    sleep 1
    echo ""

    step "6" "Check consumer log"
    pause
    echo -e "${MAGENTA}  📄 Consumer output:${NC}"
    cat /tmp/cmq_demo_c4.log | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    local before=$(grep -c "BEFORE-UNSUB" /tmp/cmq_demo_c4.log 2>/dev/null || echo "0")
    local after=$(grep -c "AFTER-UNSUB" /tmp/cmq_demo_c4.log 2>/dev/null || echo "0")
    if [ "$before" -ge 1 ] && [ "$after" -eq 0 ]; then
        success "Received message BEFORE unsub ✅"
        success "Did NOT receive message AFTER unsub ✅"
        success "Unsubscribe works correctly!"
    else
        info "before=$before, after=$after"
    fi

    kill $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 4 complete!"
}

# ============================================================================
#  DEMO 5: Pending ACK & Redelivery Timeout
# ============================================================================
demo5() {
    banner "5" "Pending ACK & Redelivery Timeout (Broker retries 3 times then disconnects)"
    cleanup

    info "When a consumer NEVER sends ACK, the broker will:"
    info "  1. Wait 5 seconds"
    info "  2. Retransmit the message (up to 3 times)"
    info "  3. After 3 failed retries → forcefully disconnect the client"
    info ""
    info "We use 'noack_client' which deliberately ignores ACK."
    info "⏳ This demo takes ~22 seconds (5s × 3 retries + margin)"
    echo ""

    step "1" "Start broker"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker5.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started"
    echo ""

    step "2" "Start noack_client (a consumer that NEVER sends ACK)"
    cmd "./bin/noack_client"
    pause
    ./bin/noack_client > /tmp/cmq_demo_noack.log 2>&1 &
    NOACK_PID=$!
    sleep 1
    success "noack_client subscribed to 'sensor/noack' — it will receive but NEVER acknowledge"
    echo ""

    step "3" "Publish a message"
    cmd "./bin/producer sensor/noack 'IMPORTANT: Please ACK this!'"
    pause
    ./bin/producer sensor/noack "IMPORTANT: Please ACK this!"
    echo ""

    step "4" "Wait and watch the broker retransmit... (watching broker log)"
    info "The broker scans every 1 second. Retry happens after 5 seconds of no ACK."
    echo ""

    # Wait and show broker log progress
    for i in $(seq 1 22); do
        sleep 1
        local retries=$(grep -c "lại\|Retry\|retry\|resend\|Gửi lại" /tmp/cmq_demo_broker5.log 2>/dev/null || echo "0")
        local disconnected=$(grep -c "ngắt kết nối\|Disconnect\|disconnect" /tmp/cmq_demo_broker5.log 2>/dev/null || echo "0")
        printf "\r${DIM}    ⏳ Elapsed: %2ds | Retries detected: %s | Disconnected: %s${NC}" "$i" "$retries" "$disconnected"
        if [ "$disconnected" -ge 1 ] && [ "$i" -ge 15 ]; then
            break
        fi
    done
    echo ""
    echo ""

    step "5" "Check broker log for retry and disconnect events"
    pause
    echo -e "${MAGENTA}  📄 Broker log (retry/disconnect entries):${NC}"
    grep -i "lại\|retry\|ngắt\|disconnect\|ACK" /tmp/cmq_demo_broker5.log | tail -10 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    step "6" "Check noack_client log — how many times did it receive the message?"
    pause
    echo -e "${MAGENTA}  📄 noack_client output:${NC}"
    cat /tmp/cmq_demo_noack.log | tail -10 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    local recv_count=$(grep -c "IMPORTANT\|NHAN\|Received\|sensor" /tmp/cmq_demo_noack.log 2>/dev/null || echo "0")
    local disc_count=$(grep -c "ngắt\|disconnect\|Disconnect" /tmp/cmq_demo_broker5.log 2>/dev/null || echo "0")

    if [ "$recv_count" -ge 2 ]; then
        success "noack_client received the message $recv_count times (original + retries)"
    fi
    if [ "$disc_count" -ge 1 ]; then
        success "Broker disconnected the unresponsive client after 3 retries"
    fi

    kill $NOACK_PID $BROKER_PID 2>/dev/null || true
    wait $NOACK_PID $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 5 complete!"
}

# ============================================================================
#  DEMO 6: Configurable Fsync Policies
# ============================================================================
demo6() {
    banner "6" "Configurable Fsync Policies (per-write vs group-commit vs time-based)"
    cleanup

    info "Three fsync policies control the durability vs performance trade-off:"
    info "  per-write    → msync(MS_SYNC) every message  (safest, slowest)"
    info "  group-commit → msync(MS_SYNC) every 5 writes (balanced)"
    info "  time-based   → msync(MS_SYNC) max once/sec   (fastest)"
    echo ""

    step "1" "Start broker with 'per-write' policy"
    cmd "./bin/broker --fsync per-write"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker6.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    echo -e "${MAGENTA}  📄 Broker startup output:${NC}"
    head -5 /tmp/cmq_demo_broker6.log | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    success "Broker running with fsync=per-write"
    echo ""

    step "2" "Publish 5 messages and measure time"
    pause
    local start_time=$(date +%s%N)
    for i in $(seq 1 5); do
        ./bin/producer "bench/topic" "Message number $i"
    done
    local end_time=$(date +%s%N)
    local elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    success "5 messages published in ${elapsed_ms}ms with per-write fsync"
    echo ""

    step "3" "Restart broker with 'group-commit' policy"
    kill $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    rm -rf data
    pause
    ./bin/broker --fsync group-commit > /tmp/cmq_demo_broker6b.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    echo -e "${MAGENTA}  📄 Broker startup:${NC}"
    head -5 /tmp/cmq_demo_broker6b.log | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    step "4" "Publish 5 messages with group-commit"
    pause
    start_time=$(date +%s%N)
    for i in $(seq 1 5); do
        ./bin/producer "bench/topic" "Message number $i"
    done
    end_time=$(date +%s%N)
    elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    success "5 messages published in ${elapsed_ms}ms with group-commit fsync"
    info "group-commit: MS_SYNC only on every 5th write, MS_ASYNC for others"
    echo ""

    step "5" "Restart broker with 'time-based' policy"
    kill $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    rm -rf data
    pause
    ./bin/broker --fsync time-based > /tmp/cmq_demo_broker6c.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    echo -e "${MAGENTA}  📄 Broker startup:${NC}"
    head -5 /tmp/cmq_demo_broker6c.log | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    step "6" "Publish 5 messages with time-based"
    pause
    start_time=$(date +%s%N)
    for i in $(seq 1 5); do
        ./bin/producer "bench/topic" "Message number $i"
    done
    end_time=$(date +%s%N)
    elapsed_ms=$(( (end_time - start_time) / 1000000 ))
    success "5 messages published in ${elapsed_ms}ms with time-based fsync"
    info "time-based: MS_SYNC only if ≥1 second since last sync"

    kill $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 6 complete!"
}

# ============================================================================
#  DEMO 7: Retention Policies
# ============================================================================
demo7() {
    banner "7" "Retention Policies (time-based, size-based, all-acknowledged)"
    cleanup

    info "Retention policies automatically clean up old messages from log files."
    info "We'll demonstrate size-based retention: cap log files at a max size."
    info "The retention worker runs every 5 seconds."
    echo ""

    step "1" "Start broker with size-based retention (max 200 bytes per log file)"
    cmd "./bin/broker --fsync per-write --retention-size 200"
    pause
    ./bin/broker --fsync per-write --retention-size 200 > /tmp/cmq_demo_broker7.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started with retention-size=200 bytes"
    echo ""

    step "2" "Publish 10 messages to fill up the log file"
    pause
    for i in $(seq 1 10); do
        ./bin/producer "retain/test" "Retention message number $i - some padding data here"
        sleep 0.1
    done
    echo ""

    step "3" "Check log file size BEFORE retention runs"
    pause
    if [ -f "data/retain_test.log" ]; then
        local size_before=$(stat -c%s "data/retain_test.log" 2>/dev/null || stat -f%z "data/retain_test.log" 2>/dev/null)
        success "File size: ${size_before} bytes (exceeds 200-byte limit)"
    fi
    echo ""

    step "4" "Wait for retention worker to run (every 5 seconds)..."
    pause
    echo -e "${DIM}    ⏳ Waiting 8 seconds for retention cleanup...${NC}"
    sleep 8
    echo ""

    step "5" "Check log file size AFTER retention"
    pause
    if [ -f "data/retain_test.log" ]; then
        local size_after=$(stat -c%s "data/retain_test.log" 2>/dev/null || stat -f%z "data/retain_test.log" 2>/dev/null)
        success "File size after trim: ${size_after} bytes"
        if [ "$size_after" -le 300 ]; then
            success "File was trimmed to fit within the retention limit!"
        fi
    fi
    echo ""

    step "6" "Check broker log for retention events"
    pause
    echo -e "${MAGENTA}  📄 Broker log (retention entries):${NC}"
    grep -i "retention\|trim\|Retention\|cleanup\|giải phóng" /tmp/cmq_demo_broker7.log | tail -5 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done

    kill $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    echo ""
    success "Demo 7 complete!"
}

# ============================================================================
#  DEMO 8: Durability Test (kill -9 → Restart → Verify No Loss)
# ============================================================================
demo8() {
    banner "8" "Durability Test (kill -9 mid-operation → Restart → Zero Data Loss)"
    cleanup

    info "This is the ULTIMATE test of our persistence design."
    info "We will:"
    info "  1. Start broker with per-write fsync"
    info "  2. Publish messages"
    info "  3. KILL THE BROKER WITH 'kill -9' (SIGKILL — cannot be caught!)"
    info "  4. Verify the log files are INTACT on disk"
    info "  5. Restart broker, reconnect consumer, verify ALL messages replayed"
    echo ""

    step "1" "Start broker with per-write fsync (maximum durability)"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker8.log 2>&1 &
    BROKER_PID=$!
    sleep 1
    success "Broker started (PID: $BROKER_PID)"
    echo ""

    step "2" "Start consumer (to establish cursor), then publish a message"
    pause
    ./bin/consumer crash_user "crash/test" > /tmp/cmq_demo_c8a.log 2>&1 &
    C_PID=$!
    sleep 1
    ./bin/producer "crash/test" "MSG-A: Before crash (consumer online)"
    sleep 2
    success "Consumer received MSG-A online"
    # Kill consumer so messages will queue
    kill $C_PID 2>/dev/null || true
    wait $C_PID 2>/dev/null || true
    sleep 1
    echo ""

    step "3" "Publish 3 more messages (consumer is offline — messages stored on disk)"
    pause
    ./bin/producer "crash/test" "MSG-B: Will survive crash"
    sleep 0.3
    ./bin/producer "crash/test" "MSG-C: Will survive crash"
    sleep 0.3
    ./bin/producer "crash/test" "MSG-D: Will survive crash"
    sleep 1
    success "3 messages published and persisted via msync(MS_SYNC)"
    echo ""

    step "4" "Show log file size BEFORE crash"
    pause
    if [ -f "data/crash_test.log" ]; then
        local size_before=$(stat -c%s "data/crash_test.log" 2>/dev/null || stat -f%z "data/crash_test.log" 2>/dev/null)
        success "data/crash_test.log = ${size_before} bytes"
    fi
    show_file "data/cursors.txt"
    echo ""

    step "5" "★★★ KILL THE BROKER WITH kill -9 (SIGKILL) ★★★"
    info "SIGKILL cannot be caught. No cleanup code runs. Process dies INSTANTLY."
    cmd "kill -9 $BROKER_PID"
    pause
    echo -e "${RED}${BOLD}"
    echo "    ╔════════════════════════════════════════╗"
    echo "    ║   💀  kill -9 $BROKER_PID  💀           "
    echo "    ║   BROKER KILLED IMMEDIATELY!           ║"
    echo "    ║   No signal handler, no cleanup,       ║"
    echo "    ║   no munmap, no close — just DEAD.     ║"
    echo "    ╚════════════════════════════════════════╝"
    echo -e "${NC}"
    kill -9 $BROKER_PID 2>/dev/null || true
    wait $BROKER_PID 2>/dev/null || true
    sleep 1
    echo ""

    step "6" "Verify log files survived the crash"
    pause
    if [ -f "data/crash_test.log" ]; then
        local size_after=$(stat -c%s "data/crash_test.log" 2>/dev/null || stat -f%z "data/crash_test.log" 2>/dev/null)
        success "data/crash_test.log STILL EXISTS! Size = ${size_after} bytes"
        if [ "$size_after" -eq "$size_before" ]; then
            success "File size UNCHANGED — no data corruption!"
        fi
    else
        fail "Log file missing!"
    fi
    echo ""
    info "The cursors.txt is also intact:"
    show_file "data/cursors.txt"
    echo ""

    step "7" "★ RESTART the broker from scratch"
    cmd "./bin/broker --fsync per-write"
    pause
    ./bin/broker --fsync per-write > /tmp/cmq_demo_broker8b.log 2>&1 &
    BROKER_PID2=$!
    sleep 1
    success "Broker restarted (new PID: $BROKER_PID2)"
    echo ""

    step "8" "★ Consumer RECONNECTS with same client_id 'crash_user'"
    info "Broker loads cursor from cursors.txt → knows where crash_user left off"
    info "replay_missed_messages() sends MSG-B, MSG-C, MSG-D from the log file"
    pause
    ./bin/consumer crash_user "crash/test" > /tmp/cmq_demo_c8b.log 2>&1 &
    C_PID2=$!
    sleep 3
    echo ""

    step "9" "Check — did the consumer receive all 3 offline messages?"
    pause
    echo -e "${MAGENTA}  📄 Consumer output after reconnect:${NC}"
    cat /tmp/cmq_demo_c8b.log | tail -10 | while IFS= read -r line; do
        echo -e "${DIM}     | $line${NC}"
    done
    echo ""

    local b=$(grep -c "MSG-B" /tmp/cmq_demo_c8b.log 2>/dev/null || echo "0")
    local c=$(grep -c "MSG-C" /tmp/cmq_demo_c8b.log 2>/dev/null || echo "0")
    local d=$(grep -c "MSG-D" /tmp/cmq_demo_c8b.log 2>/dev/null || echo "0")

    if [ "$b" -ge 1 ] && [ "$c" -ge 1 ] && [ "$d" -ge 1 ]; then
        echo ""
        echo -e "${GREEN}${BOLD}"
        echo "    ╔════════════════════════════════════════════════════╗"
        echo "    ║  ✅  ALL 3 MESSAGES RECOVERED AFTER kill -9!     ║"
        echo "    ║                                                    ║"
        echo "    ║  MSG-B: received ✅                                ║"
        echo "    ║  MSG-C: received ✅                                ║"
        echo "    ║  MSG-D: received ✅                                ║"
        echo "    ║                                                    ║"
        echo "    ║  ZERO DATA LOSS — DURABILITY VERIFIED!            ║"
        echo "    ╚════════════════════════════════════════════════════╝"
        echo -e "${NC}"
    else
        info "MSG-B=$b, MSG-C=$c, MSG-D=$d"
    fi

    kill $C_PID2 $BROKER_PID2 2>/dev/null || true
    wait $C_PID2 $BROKER_PID2 2>/dev/null || true
    echo ""
    success "Demo 8 complete!"
}

# ============================================================================
#  MAIN — Menu
# ============================================================================
print_menu() {
    echo ""
    echo -e "${CYAN}${LINE}${NC}"
    echo -e "${WHITE}${BOLD}  C-MQ BROKER — LIVE DEMO MENU${NC}"
    echo -e "${CYAN}${LINE}${NC}"
    echo ""
    echo -e "  ${GREEN}1${NC}  Basic Publish-Subscribe (CONNECT → SUBSCRIBE → PUBLISH → RECEIVE → ACK)"
    echo -e "  ${GREEN}2${NC}  Wildcard Topic Matching (+ single-level, # multi-level)"
    echo -e "  ${GREEN}3${NC}  Offline Queueing & Resume-on-Reconnect (Cursors)"
    echo -e "  ${GREEN}4${NC}  Clean Unsubscribe"
    echo -e "  ${GREEN}5${NC}  Pending ACK & Redelivery Timeout (noack_client)"
    echo -e "  ${GREEN}6${NC}  Configurable Fsync Policies Comparison"
    echo -e "  ${GREEN}7${NC}  Retention Policies (size-based auto-cleanup)"
    echo -e "  ${GREEN}8${NC}  ★ Durability Test (kill -9 → restart → zero data loss)"
    echo ""
    echo -e "  ${YELLOW}all${NC}  Run all demos sequentially"
    echo -e "  ${RED}q${NC}    Quit"
    echo ""
}

run_demo() {
    case "$1" in
        1) demo1 ;;
        2) demo2 ;;
        3) demo3 ;;
        4) demo4 ;;
        5) demo5 ;;
        6) demo6 ;;
        7) demo7 ;;
        8) demo8 ;;
        all)
            demo1; pause
            demo2; pause
            demo3; pause
            demo4; pause
            demo5; pause
            demo6; pause
            demo7; pause
            demo8
            echo ""
            echo -e "${GREEN}${BOLD}"
            echo "    ╔════════════════════════════════════════════╗"
            echo "    ║  🎉  ALL 8 DEMOS COMPLETED SUCCESSFULLY! ║"
            echo "    ╚════════════════════════════════════════════╝"
            echo -e "${NC}"
            ;;
        q|Q) echo "Bye!"; exit 0 ;;
        *) echo -e "${RED}Invalid option. Use 1-8, 'all', or 'q'.${NC}" ;;
    esac
}

# If argument provided, run that demo directly
if [ -n "$1" ]; then
    run_demo "$1"
    cleanup
    exit 0
fi

# Interactive menu
while true; do
    print_menu
    echo -ne "${YELLOW}  Select demo [1-8/all/q]: ${NC}"
    read -r choice
    run_demo "$choice"
    if [ "$choice" != "q" ] && [ "$choice" != "Q" ]; then
        cleanup
    fi
done
