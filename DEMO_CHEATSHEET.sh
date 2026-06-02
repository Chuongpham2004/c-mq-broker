# C-MQ Broker — Manual Demo Commands (Cheat Sheet)
# Copy-paste these into separate terminal tabs during live defense
# ================================================================

# ======================== PREPARATION ============================
# Terminal 0: Build everything
make clean && make all
rm -rf data
killall broker consumer noack_client 2>/dev/null


# ================================================================
# DEMO 1: Basic Pub-Sub
# ================================================================

# Terminal 1: Start broker
./bin/broker --fsync per-write

# Terminal 2: Start consumer
./bin/consumer client_A sensor/nhietdo

# Terminal 3: Publish message
./bin/producer sensor/nhietdo "Temperature: 32.5 degrees"

# Check: consumer should print the received message
# Check: ls -la data/   → sensor_nhietdo.log exists
# Check: cat data/cursors.txt  → shows client_A's offset

# Ctrl+C all terminals before next demo
# rm -rf data


# ================================================================
# DEMO 2: Wildcard Matching
# ================================================================

# Terminal 1: Broker
./bin/broker --fsync per-write

# Terminal 2: Consumer A — single-level wildcard (+)
./bin/consumer user_A "sensor/+/temp"

# Terminal 3: Consumer B — multi-level wildcard (#)
./bin/consumer user_B "sensor/#"

# Terminal 4: Publish to 2-level topic
./bin/producer sensor/room1/temp "Room1: 25C"
# → Consumer A receives ✅ (+ = room1)
# → Consumer B receives ✅ (# = room1/temp)

# Terminal 4: Publish to 3-level topic
./bin/producer sensor/room1/room2/temp "Deep: 28C"
# → Consumer A does NOT receive ❌ (+ only matches 1 level)
# → Consumer B receives ✅ (# = room1/room2/temp)


# ================================================================
# DEMO 3: Offline Resume (Cursors)
# ================================================================

# Terminal 1: Broker
./bin/broker --fsync per-write

# Terminal 2: Consumer connects
./bin/consumer device_01 data/sensor

# Terminal 3: Send a message (consumer receives it)
./bin/producer data/sensor "MSG-1: Online"

# NOW: Ctrl+C the consumer in Terminal 2 (disconnect)

# Terminal 3: Send 2 more messages while consumer is OFFLINE
./bin/producer data/sensor "MSG-2: Offline message"
./bin/producer data/sensor "MSG-3: Offline message"

# Check cursor: cat data/cursors.txt

# Terminal 2: Reconnect consumer with SAME client_id
./bin/consumer device_01 data/sensor
# → Consumer receives MSG-2 and MSG-3 automatically (replayed from log)


# ================================================================
# DEMO 4: Unsubscribe
# ================================================================

# Terminal 1: Broker
./bin/broker --fsync per-write

# Terminal 2: Consumer
./bin/consumer reader_1 news/tech

# Terminal 3: Publish (consumer receives this)
./bin/producer news/tech "BEFORE: You should see this"

# Terminal 2: Press Ctrl+C → sends UNSUBSCRIBE then exits

# Terminal 3: Publish again (no one receives this)
./bin/producer news/tech "AFTER: No one should see this"


# ================================================================
# DEMO 5: Pending ACK & Redelivery (takes ~22 seconds)
# ================================================================

# Terminal 1: Broker
./bin/broker --fsync per-write

# Terminal 2: NoACK client (never sends ACK)
./bin/noack_client

# Terminal 3: Publish
./bin/producer sensor/noack "Please ACK me!"

# Wait ~22 seconds and watch Terminal 1 (broker):
# → "Gửi lại tin nhắn ID ... (lần 1)"   ← retry 1 at ~5s
# → "Gửi lại tin nhắn ID ... (lần 2)"   ← retry 2 at ~10s
# → "Gửi lại tin nhắn ID ... (lần 3)"   ← retry 3 at ~15s
# → "ngắt kết nối..."                     ← disconnect at ~20s

# Terminal 2: noack_client will show the message received multiple times


# ================================================================
# DEMO 6: Fsync Policy Comparison
# ================================================================

# Per-write (safest, slowest):
./bin/broker --fsync per-write

# Group-commit (balanced):
./bin/broker --fsync group-commit

# Time-based (fastest):
./bin/broker --fsync time-based

# Compare: publish 10 messages with each policy and observe broker log


# ================================================================
# DEMO 7: Retention (size-based)
# ================================================================

# Terminal 1: Broker with 200-byte size limit
./bin/broker --fsync per-write --retention-size 200

# Terminal 2: Publish 10 messages
for i in $(seq 1 10); do
    ./bin/producer retain/test "Retention message $i with padding data"
done

# Check file size:
ls -la data/retain_test.log
# → File will be large (>200 bytes)

# Wait 8 seconds for retention worker...
sleep 8

# Check again:
ls -la data/retain_test.log
# → File has been trimmed!

# Broker log shows: "Retention cleanup hoàn tất... đã giải phóng N bytes"


# ================================================================
# DEMO 8: Durability (kill -9) ★ THE BIG ONE ★
# ================================================================

# Terminal 1: Start broker
./bin/broker --fsync per-write
# Note the PID (shown in terminal or use: echo $!)

# Terminal 2: Consumer online briefly
./bin/consumer crash_user crash/test
# Receive one message, then Ctrl+C to disconnect

# Terminal 3: Publish while consumer is offline
./bin/producer crash/test "MSG-1: Will survive crash"
./bin/producer crash/test "MSG-2: Will survive crash"
./bin/producer crash/test "MSG-3: Will survive crash"

# Verify data on disk:
ls -la data/crash_test.log
cat data/cursors.txt

# ★ THE KILL ★
# Terminal 4:
kill -9 $(pgrep broker)

# Verify files survived:
ls -la data/crash_test.log    # Still exists!
cat data/cursors.txt           # Still intact!

# Restart broker:
./bin/broker --fsync per-write

# Reconnect consumer with SAME client_id:
./bin/consumer crash_user crash/test
# → Receives MSG-1, MSG-2, MSG-3 automatically
# → ZERO DATA LOSS! ✅
