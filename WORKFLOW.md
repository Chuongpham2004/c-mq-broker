# C-MQ Broker — Complete System Workflow

This document describes the end-to-end workflow of the C-MQ Broker system, detailing exactly how each module participates in every phase of operation — from startup to crash recovery.

---

## Phase 1: Broker Startup

**Entry point:** `main()` in `src/server.c`  
**Trigger:** Running `./bin/broker [options]`

```
./bin/broker --fsync per-write --retention-time 3600 --retention-acked
```

### Step-by-step execution:

```
main()
  │
  ├─ 1. init_broker()                              [broker.c:84]
  │     ├─ Zero out broker.topics[] array
  │     ├─ Initialize broker.global_lock mutex
  │     └─ init_storage()                           [storage.c:19]
  │           └─ Create ./data/ directory if not exists (mkdir, 0700)
  │
  ├─ 2. Parse CLI arguments
  │     ├─ --fsync per-write|group-commit|time-based
  │     │     └─ Sets global_fsync_policy (enum in storage.h)
  │     ├─ --retention-time <seconds>
  │     │     └─ Sets global_retention_time
  │     ├─ --retention-size <bytes>
  │     │     └─ Sets global_retention_size
  │     └─ --retention-acked
  │           └─ Sets global_retention_acked = 1
  │
  ├─ 3. init_broker_threads()                       [broker.c:858]
  │     ├─ Spawn redelivery_worker thread (detached)
  │     │     └─ Scans pending-ACK list every 1 second
  │     ├─ Spawn retention_worker thread (detached)
  │     │     └─ Trims log files every 5 seconds
  │     └─ load_cursors()                           [broker.c:554]
  │           └─ Read data/cursors.txt → populate cursor_records[] array
  │              Format: client_id:topic:offset (one per line)
  │
  ├─ 4. Register signal handlers via sigaction()
  │     ├─ SIGINT  (Ctrl+C) → handle_signal()
  │     ├─ SIGTERM (kill)   → handle_signal()
  │     └─ sa_flags = 0 (SA_RESTART disabled → accept() interrupted by EINTR)
  │
  ├─ 5. create_server_socket(8080)                  [network.c:11]
  │     ├─ socket(AF_INET, SOCK_STREAM, 0)
  │     ├─ setsockopt(SO_REUSEADDR)
  │     ├─ bind(INADDR_ANY, port 8080)
  │     └─ listen(backlog = 128)
  │
  ├─ 6. threadpool_create(4, 100)                   [threadpool.c:38]
  │     ├─ Allocate circular queue (100 task slots)
  │     ├─ Initialize mutex + condition variable
  │     └─ Spawn 4 worker threads
  │           └─ Each worker: lock → wait on cond_var → sleep until task arrives
  │
  └─ 7. Enter accept() loop
        └─ while (keep_running) {
              client_socket = accept(server_socket, ...);
              threadpool_add(pool, handle_client, client_socket);
           }
```

**System state after startup:**

```
┌─────────────────────────────────────────────────┐
│ Broker Process                                  │
│                                                 │
│  Main Thread ──────── accept() [blocking]       │
│                                                 │
│  Worker #1 ────────── cond_wait [sleeping]       │
│  Worker #2 ────────── cond_wait [sleeping]       │
│  Worker #3 ────────── cond_wait [sleeping]       │
│  Worker #4 ────────── cond_wait [sleeping]       │
│                                                 │
│  Redelivery Worker ── sleep(1) [looping]        │
│  Retention Worker ─── sleep(5) [looping]        │
│                                                 │
│  Topics: 0    Sessions: 0    PendingACKs: 0     │
│  Cursors: loaded from data/cursors.txt          │
└─────────────────────────────────────────────────┘
```

---

## Phase 2: Client Connection (MSG_CONNECT)

**Trigger:** Client runs `./bin/consumer my_client sensor/nhietdo` or `./bin/producer ...`

### Client-side (consumer.c or producer.c):

```
1. socket(AF_INET, SOCK_STREAM, 0)
2. connect(broker_ip:8080)
3. Build MSG_CONNECT message:
     Header: type=1, topic_len=0, msg_id=0, timestamp=now, payload_len=strlen(client_id)
     Payload: "my_client" (the client_id string)
4. send(header + payload)
```

### Broker-side:

```
Main Thread (Boss):
  accept() returns → new client_socket
  │
  ├─ malloc(sizeof(int)), store client_socket
  └─ threadpool_add(pool, handle_client, pclient)
       │
       └─ Enqueue task into circular buffer
          Signal cond_var → Wake one sleeping worker

Worker Thread (wakes up):
  │
  ├─ Dequeue task from circular buffer
  ├─ client_socket = *(int *)arg; free(arg);
  └─ Enter handle_client() loop:               [server.c:30]
       │
       └─ receive_message(client_socket)         [network.c:76]
            ├─ read_exact(fd, &header, 14)  → Read 14-byte header
            ├─ ntohl(msg_id, timestamp, payload_len)
            ├─ read_exact(fd, topic, topic_len)   → (empty for CONNECT)
            └─ read_exact(fd, payload, payload_len) → "my_client"
            
       Dispatch: header.type == MSG_CONNECT
       │
       └─ broker_register_session(socket, "my_client")  [broker.c:93]
            ├─ Lock session_lock
            ├─ Remove any old session with same socket or client_id
            ├─ active_sessions[count] = { socket, "my_client" }
            ├─ active_session_count++
            └─ Unlock session_lock
```

**System state after CONNECT:**

```
Active Sessions: [ { socket=5, client_id="my_client" } ]
```

---

## Phase 3: Consumer Subscribes (MSG_SUBSCRIBE)

**Trigger:** Consumer sends MSG_SUBSCRIBE with topic name (may contain wildcards)

### Broker-side flow:

```
receive_message() → header.type == MSG_SUBSCRIBE, topic = "sensor/+"
  │
  └─ broker_subscribe(client_socket=5, "sensor/+")      [broker.c:263]
       │
       ├─ 1. find_or_create_topic("sensor/+")             [broker.c:148]
       │     ├─ Lock broker.global_lock
       │     ├─ Scan broker.topics[0..topic_count-1] for name match
       │     ├─ If not found AND topic_count < 64:
       │     │     ├─ Copy name "sensor/+" into topics[topic_count]
       │     │     ├─ Initialize topic_lock mutex
       │     │     ├─ topic_count++
       │     │     └─ Return pointer to new topic
       │     └─ Unlock broker.global_lock
       │
       ├─ 2. Add subscriber to topic
       │     ├─ Lock topic->topic_lock
       │     ├─ Check for duplicate (skip if socket already listed)
       │     ├─ topic->subscriber_sockets[sub_count] = 5
       │     ├─ topic->sub_count++
       │     └─ Unlock topic->topic_lock
       │
       └─ 3. replay_missed_messages(socket=5, "my_client", "sensor/+")
                                                          [broker.c:444]
             │
             ├─ opendir("./data/")
             ├─ For each *.log file in data/:
             │     │
             │     ├─ Read original topic name from first record header
             │     │   (e.g., file "sensor_nhietdo.log" → topic "sensor/nhietdo")
             │     │
             │     ├─ match_topic("sensor/+", "sensor/nhietdo")  → MATCH ✓
             │     │
             │     ├─ cursor = get_cursor("my_client", "sensor/nhietdo")
             │     │   └─ Search cursor_records[] → e.g., returns 456 (bytes)
             │     │
             │     ├─ fd = open("data/sensor_nhietdo.log", O_RDONLY)
             │     ├─ fstat(fd) → file_size = 1200 bytes
             │     ├─ Since file_size(1200) > cursor(456) → has unread data
             │     │
             │     ├─ map = mmap(file, PROT_READ, MAP_SHARED)
             │     ├─ offset = 456 (start from cursor position)
             │     │
             │     └─ while (offset + 14 <= file_size):
             │           │
             │           ├─ Parse MessageHeader at map+offset
             │           │   record_size = 14 + topic_len + payload_len
             │           │
             │           ├─ Assign new unique msg_id
             │           │
             │           ├─ Create PendingAck node:
             │           │   { socket=5, msg_id=42, client_id="my_client",
             │           │     topic="sensor/nhietdo", file_offset=offset+record_size,
             │           │     payload=copy_of_data, sent_time=now, retry_count=0 }
             │           │
             │           ├─ Insert node at head of pending_ack linked list
             │           │
             │           ├─ send(socket, header + topic + payload)
             │           │   → Consumer receives the replayed message
             │           │
             │           └─ offset += record_size (advance to next record)
             │
             ├─ munmap() + close()
             └─ closedir()
```

**System state after SUBSCRIBE:**

```
Topics: [ { name="sensor/+", subscribers=[socket 5], sub_count=1 } ]
PendingACK list: [msg42] → [msg43] → [msg44] → NULL  (replayed msgs)
Consumer: receiving replayed messages, must ACK each one
```

---

## Phase 4: Producer Publishes (MSG_PUBLISH)

**Trigger:** `./bin/producer sensor/nhietdo "32.5 degrees"`

### Producer-side:

```
1. connect(broker:8080)
2. Build MSG_PUBLISH message:
     Header: type=2, topic_len=15, msg_id=0, timestamp=now, payload_len=12
     Topic:  "sensor/nhietdo"
     Payload: "32.5 degrees"
3. send(header + topic + payload)
4. close(socket)  → fire-and-forget
```

### Broker-side:

```
Worker thread receives MSG_PUBLISH:
  │
  └─ broker_publish(msg)                                 [broker.c:177]
       │
       ├─ 1. PERSIST TO DISK
       │     │
       │     └─ storage_save_message("sensor/nhietdo", msg)  [storage.c:28]
       │          │
       │          ├─ Lock storage_mutex
       │          ├─ Convert topic → filename: "sensor/nhietdo" → "data/sensor_nhietdo.log"
       │          ├─ fd = open("data/sensor_nhietdo.log", O_RDWR | O_CREAT)
       │          ├─ fstat(fd) → old_size = 456
       │          ├─ record_size = 14 + 15 + 12 = 41 bytes
       │          ├─ new_size = 456 + 41 = 497
       │          │
       │          ├─ ftruncate(fd, 497)          ← Extend file FIRST
       │          ├─ mmap(NULL, 497, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)
       │          │
       │          ├─ Write at offset 456:
       │          │   ┌──────────────────┬─────────────────┬──────────────┐
       │          │   │  Header (14 B)   │  Topic (15 B)   │ Payload(12B) │
       │          │   │ type=2           │"sensor/nhietdo" │"32.5 degrees"│
       │          │   │ topic_len=15     │                 │              │
       │          │   │ msg_id (NBO)     │                 │              │
       │          │   │ timestamp (NBO)  │                 │              │
       │          │   │ payload_len (NBO)│                 │              │
       │          │   └──────────────────┴─────────────────┴──────────────┘
       │          │
       │          ├─ FSYNC POLICY:
       │          │   ├─ per-write:     msync(map, 497, MS_SYNC)   ← blocks until on disk
       │          │   ├─ group-commit:  if (count % 5 == 0) MS_SYNC else MS_ASYNC
       │          │   └─ time-based:    if (now - last >= 1s) MS_SYNC else MS_ASYNC
       │          │
       │          ├─ munmap(map, 497)
       │          ├─ close(fd)
       │          ├─ Unlock storage_mutex
       │          └─ return 497 (end_offset → used as cursor position)
       │
       ├─ 2. WILDCARD FAN-OUT
       │     │
       │     ├─ Lock broker.global_lock
       │     ├─ Initialize sent_sockets[] = {} (dedup array)
       │     │
       │     └─ for each topic in broker.topics[]:
       │           │
       │           ├─ match_topic(topic.name, "sensor/nhietdo")
       │           │
       │           │   Example matches:
       │           │     "sensor/nhietdo" → exact match     ✓
       │           │     "sensor/+"       → + = nhietdo     ✓
       │           │     "sensor/#"       → # = nhietdo     ✓
       │           │     "#"              → matches all     ✓
       │           │     "sensor/doam"    → no match        ✗
       │           │     "sensor/+/sub"   → no match        ✗
       │           │
       │           └─ If MATCH:
       │                 Lock topic->topic_lock
       │                 │
       │                 for each subscriber socket in topic:
       │                 │
       │                 ├─ Check dedup: is socket in sent_sockets[]?
       │                 │   └─ YES → skip (prevent duplicate delivery)
       │                 │   └─ NO  → continue
       │                 │
       │                 ├─ Add socket to sent_sockets[]
       │                 │
       │                 ├─ Generate unique msg_id = get_next_global_msg_id()
       │                 │   └─ Lock msg_id_lock → id = counter++ → unlock
       │                 │
       │                 ├─ Look up client_id from socket
       │                 │   └─ broker_get_client_id(socket) → "my_client"
       │                 │
       │                 ├─ CREATE PENDING ACK NODE:
       │                 │   ├─ Lock pending_ack_lock
       │                 │   ├─ node = malloc(PendingAck)
       │                 │   ├─ node.client_socket = 5
       │                 │   ├─ node.msg_id = 100
       │                 │   ├─ node.client_id = "my_client"
       │                 │   ├─ node.topic = "sensor/nhietdo"
       │                 │   ├─ node.file_offset = 497  (end of record)
       │                 │   ├─ node.payload = malloc + memcpy(payload)
       │                 │   ├─ node.sent_time = now
       │                 │   ├─ node.retry_count = 0
       │                 │   ├─ node.next = pending_ack_head
       │                 │   ├─ pending_ack_head = node
       │                 │   └─ Unlock pending_ack_lock
       │                 │
       │                 ├─ SEND TO CONSUMER:
       │                 │   ├─ Build header: type=2, topic_len=15,
       │                 │   │   msg_id=htonl(100), timestamp=htonl(now),
       │                 │   │   payload_len=htonl(12)
       │                 │   ├─ send(socket, &header, 14, 0)
       │                 │   ├─ send(socket, "sensor/nhietdo", 15, 0)
       │                 │   └─ send(socket, "32.5 degrees", 12, 0)
       │                 │
       │                 └─ Unlock topic->topic_lock
       │
       └─ Unlock broker.global_lock
```

---

## Phase 5: Consumer Receives Message and Sends ACK

**Trigger:** TCP data arrives at consumer's socket

### Consumer-side (consumer.c):

```
receive_message(socket)
  │
  ├─ read_exact(socket, &header, 14)
  ├─ ntohl(msg_id, timestamp, payload_len)
  ├─ read_exact(socket, topic, 15)   → "sensor/nhietdo"
  ├─ read_exact(socket, payload, 12) → "32.5 degrees"
  │
  ├─ Print: "[RECEIVED] Topic: sensor/nhietdo → Payload: 32.5 degrees"
  │
  └─ Send ACK back to broker:
       ├─ Build ACK header:
       │   type=4 (MSG_ACK), topic_len=0,
       │   msg_id=htonl(100), timestamp=htonl(now), payload_len=0
       └─ send(socket, &ack_header, 14, 0)
```

### Broker-side — Processing ACK:

```
Worker thread: receive_message() → header.type == MSG_ACK, msg_id = 100
  │
  └─ broker_handle_ack(socket=5, msg_id=100)            [broker.c:351]
       │
       ├─ Lock pending_ack_lock
       │
       ├─ Traverse linked list: pending_ack_head → ...
       │   Find node where (client_socket==5 AND msg_id==100)
       │
       ├─ UPDATE CURSOR:
       │   └─ set_cursor("my_client", "sensor/nhietdo", 497)  [broker.c:601]
       │        ├─ Lock cursor_lock
       │        ├─ Search cursor_records[] for matching (client_id, topic)
       │        ├─ If found AND 497 > existing_offset:
       │        │     cursor_records[i].offset = 497
       │        ├─ If not found:
       │        │     cursor_records[cursor_count++] = {"my_client","sensor/nhietdo",497}
       │        ├─ Unlock cursor_lock
       │        └─ save_cursors()                        [broker.c:575]
       │             ├─ Lock cursor_lock
       │             ├─ fopen("data/cursors.txt", "w")
       │             ├─ Write all records: "client_id:topic:offset\n"
       │             ├─ fclose()
       │             └─ Unlock cursor_lock
       │
       ├─ REMOVE NODE FROM LINKED LIST:
       │   ├─ if (prev == NULL) pending_ack_head = node->next
       │   └─ else prev->next = node->next
       │
       ├─ FREE MEMORY:
       │   ├─ free(node->payload)
       │   └─ free(node)
       │
       └─ Unlock pending_ack_lock
```

**data/cursors.txt after ACK:**

```
my_client:sensor/nhietdo:497
```

---

## Phase 6: Redelivery Worker (Background Thread)

**Thread:** `redelivery_worker()` in `broker.c:627`  
**Runs:** Every 1 second, continuously

```
while (1) {
    sleep(1);
    │
    ├─ Lock pending_ack_lock
    ├─ now = time(NULL)
    │
    ├─ Traverse pending_ack linked list:
    │
    │   For each node:
    │     elapsed = now - node.sent_time
    │
    │     if (elapsed < 5 seconds):
    │       └─ Skip (still within timeout window)
    │
    │     if (elapsed >= 5 seconds):
    │       │
    │       ├─ node.retry_count++
    │       │
    │       ├─ if (retry_count > 3):
    │       │     │
    │       │     ├─ CLIENT IS UNRESPONSIVE — FORCE DISCONNECT
    │       │     ├─ shutdown(node.client_socket, SHUT_RDWR)
    │       │     │   └─ Forces any thread blocked on read() for this socket
    │       │     │      to return immediately with ECONNRESET
    │       │     ├─ close(node.client_socket)
    │       │     ├─ Remove node from linked list
    │       │     └─ free(node.payload), free(node)
    │       │
    │       └─ if (retry_count <= 3):
    │             │
    │             ├─ RETRANSMIT MESSAGE:
    │             │   ├─ Build header with same msg_id, new timestamp
    │             │   ├─ send(socket, header, 14)
    │             │   ├─ send(socket, topic, topic_len)
    │             │   └─ send(socket, payload, payload_len)
    │             │
    │             └─ node.sent_time = now (reset timeout timer)
    │
    └─ Unlock pending_ack_lock
}
```

**Timeline example for an unresponsive client:**

```
T=0s   Message sent to consumer. PendingAck created. retry_count=0
T=5s   No ACK received. retry_count=1. Message retransmitted.
T=10s  No ACK received. retry_count=2. Message retransmitted.
T=15s  No ACK received. retry_count=3. Message retransmitted.
T=20s  No ACK received. retry_count=4 > 3. shutdown() + close(). Node removed.
```

---

## Phase 7: Retention Worker (Background Thread)

**Thread:** `retention_worker()` in `broker.c:838`  
**Runs:** Every 5 seconds, continuously

```
while (1) {
    sleep(5);
    │
    ├─ opendir("./data/")
    │
    └─ For each *.log file:
         │
         └─ process_retention_for_file(filename)         [broker.c:680]
              │
              ├─ Open file with mmap(PROT_READ, MAP_SHARED)
              ├─ Read original topic name from first record's header
              │
              ├─ POLICY 1: TIME-BASED (if --retention-time N)
              │   │
              │   ├─ cutoff = now - N seconds
              │   ├─ Scan records from front of file:
              │   │   while (record.timestamp < cutoff):
              │   │     time_bytes_to_remove += record_size
              │   └─ Stop at first record newer than cutoff
              │
              ├─ POLICY 2: SIZE-BASED (if --retention-size N)
              │   │
              │   ├─ if file_size > N bytes:
              │   │   Scan records from front:
              │   │   while (file_size - offset > N):
              │   │     size_bytes_to_remove += record_size
              │   └─ Stop when remaining data fits within limit
              │
              ├─ POLICY 3: ALL-ACKED (if --retention-acked)
              │   │
              │   ├─ Find min(cursor_offset) across ALL subscribers of this topic
              │   │   Lock cursor_lock
              │   │   for each cursor where topic matches:
              │   │     min_offset = min(min_offset, cursor.offset)
              │   │   Unlock cursor_lock
              │   │
              │   ├─ Scan records from front:
              │   │   while (offset + record_size <= min_offset):
              │   │     acked_bytes_to_remove += record_size
              │   └─ These records have been ACKed by ALL subscribers → safe to remove
              │
              ├─ COMPUTE FINAL TRIM:
              │   bytes_to_remove = MAX(time_bytes, size_bytes, acked_bytes)
              │
              ├─ munmap() + close()
              │
              ├─ if bytes_to_remove > 0:
              │   │
              │   ├─ trim_log_file(filepath, bytes_to_remove)  [storage.c:139]
              │   │   ├─ Lock storage_mutex
              │   │   ├─ Open file, mmap entire content
              │   │   ├─ memmove(map, map + bytes_to_remove, new_size)
              │   │   │   └─ Shift remaining data to front of file
              │   │   ├─ msync(MS_SYNC) → ensure shifted data is on disk
              │   │   ├─ munmap()
              │   │   ├─ ftruncate(fd, new_size) → shrink file
              │   │   ├─ close(fd)
              │   │   └─ Unlock storage_mutex
              │   │
              │   ├─ ADJUST CURSOR OFFSETS:
              │   │   Lock cursor_lock
              │   │   for each cursor matching this topic:
              │   │     if (cursor.offset < bytes_to_remove)
              │   │       cursor.offset = 0
              │   │     else
              │   │       cursor.offset -= bytes_to_remove
              │   │   Unlock cursor_lock
              │   │   save_cursors() → rewrite data/cursors.txt
              │   │
              │   └─ ADJUST PENDING-ACK OFFSETS:
              │       Lock pending_ack_lock
              │       for each PendingAck node matching this topic:
              │         if (node.file_offset < bytes_to_remove)
              │           node.file_offset = 0
              │         else
              │           node.file_offset -= bytes_to_remove
              │       Unlock pending_ack_lock
              │
              └─ Continue to next .log file
}
```

**Before and after trim (visual):**

```
BEFORE (file = 1000 bytes, bytes_to_remove = 400):
┌──────────────────────────────────────────────────────────┐
│ Record A (100B) │ Record B (150B) │ Record C (150B) │ Record D (200B) │ Record E (400B) │
│  OLD - remove   │  OLD - remove   │  OLD - remove   │    KEEP         │     KEEP        │
└──────────────────────────────────────────────────────────┘
                                     ▲ bytes_to_remove = 400

AFTER trim_log_file() (file = 600 bytes):
┌──────────────────────────────┐
│ Record D (200B) │ Record E (400B) │
│     KEEP        │     KEEP        │
└──────────────────────────────┘
All cursor offsets reduced by 400.
```

---

## Phase 8: Consumer Unsubscribe (MSG_UNSUBSCRIBE)

**Trigger:** Consumer sends MSG_UNSUBSCRIBE, or presses Ctrl+C (consumer.c handles SIGINT by sending UNSUBSCRIBE before exit)

### Broker-side:

```
receive_message() → header.type == MSG_UNSUBSCRIBE, topic = "sensor/+"
  │
  └─ broker_unsubscribe(socket=5, "sensor/+")            [broker.c:298]
       │
       ├─ Lock broker.global_lock
       ├─ Find topic with name "sensor/+"
       ├─ Lock topic->topic_lock
       │
       ├─ Find socket 5 in subscriber_sockets[]
       ├─ Remove by shifting array elements left:
       │   subscriber_sockets[j] = subscriber_sockets[j+1]  (for j=found..sub_count-2)
       ├─ sub_count--
       │
       ├─ Unlock topic->topic_lock
       └─ Unlock broker.global_lock

Note: Cursor is PRESERVED. If the consumer re-subscribes later,
      it will resume from where it left off.
```

---

## Phase 9: Client Disconnect

**Trigger:** Consumer closes socket, or broker detects read() returns 0 (EOF)

### Broker-side:

```
Worker thread: receive_message() returns NULL (client disconnected)
  │
  ├─ broker_remove_client(socket=5)                      [broker.c:324]
  │   │
  │   ├─ 1. REMOVE FROM ALL TOPICS:
  │   │     Lock broker.global_lock
  │   │     for each topic:
  │   │       Lock topic->topic_lock
  │   │       if socket 5 in subscriber_sockets[]:
  │   │         remove it, sub_count--
  │   │       Unlock topic->topic_lock
  │   │     Unlock broker.global_lock
  │   │
  │   ├─ 2. REMOVE SESSION:
  │   │     broker_remove_session(socket=5)
  │   │     Lock session_lock
  │   │     Remove from active_sessions[], session_count--
  │   │     Unlock session_lock
  │   │
  │   └─ 3. CLEANUP PENDING ACKs:
  │         cleanup_pending_acks_for_socket(socket=5)    [broker.c:382]
  │         Lock pending_ack_lock
  │         Remove ALL PendingAck nodes where client_socket == 5
  │         free(payload), free(node) for each
  │         Unlock pending_ack_lock
  │         (Cursors NOT updated — messages were not acknowledged)
  │
  └─ close(socket=5)
```

---

## Phase 10: Graceful Shutdown (SIGINT / SIGTERM)

**Trigger:** Operator presses Ctrl+C or runs `kill <broker_pid>`

```
Signal delivered to broker process:
  │
  └─ handle_signal(SIGINT)                               [server.c:22]
       └─ keep_running = 0   (volatile sig_atomic_t)

Main thread (blocked in accept()):
  │
  ├─ accept() interrupted → returns -1, errno = EINTR
  │   (because SA_RESTART was NOT set)
  │
  ├─ if (errno == EINTR) break;
  │
  └─ EXIT MAIN LOOP

Cleanup:
  │
  ├─ close(server_socket)
  │   └─ No new connections accepted
  │
  ├─ Worker threads handling active clients:
  │   Their clients' read_exact() will eventually return 0 or -1
  │   → Each worker calls broker_remove_client() + close()
  │   → Workers return to the pool (wait on cond_var)
  │
  ├─ Background threads (detached):
  │   Continue running until process exits
  │   (No explicit cleanup needed — OS reclaims resources)
  │
  └─ main() returns 0

All mmap regions were already munmap'd within their respective functions.
All cursors were saved on last ACK.
Data files on disk are intact.
```

---

## Phase 11: Crash Recovery (kill -9 → Restart)

**Trigger:** `kill -9 <broker_pid>` (SIGKILL — cannot be caught or handled)

### What happens during kill -9:

```
Process terminated IMMEDIATELY by kernel.
  │
  ├─ No signal handler runs (SIGKILL cannot be caught)
  ├─ No cleanup code executes
  ├─ No munmap(), no close(), no save_cursors()
  │
  ├─ BUT: with --fsync per-write mode:
  │   │
  │   │  For every message written BEFORE the kill:
  │   │    ftruncate() → file extended on disk           ✓
  │   │    memcpy()    → data written to page cache      ✓
  │   │    msync(MS_SYNC) → page cache flushed to disk   ✓
  │   │    (msync returned → data is on physical storage)
  │   │
  │   └─ Therefore: ALL data that was msync'd is SAFE on disk
  │
  └─ Kernel reclaims all process resources (memory, file descriptors, mappings)
     File contents remain on disk exactly as last msync'd.
```

### What survives on disk:

```
data/
  ├─ sensor_nhietdo.log    ← Complete, all msync'd records intact
  ├─ home_light.log        ← Complete, all msync'd records intact
  └─ cursors.txt           ← Last saved state (from most recent ACK)
```

### Recovery on restart:

```
./bin/broker --fsync per-write          ← Restart broker
  │
  ├─ init_broker()                      ← Fresh in-memory state
  ├─ init_broker_threads()
  │   └─ load_cursors()                 ← Reload cursors from data/cursors.txt
  │       Each client's last ACK'd position is restored
  │
  └─ Broker is now ready

./bin/consumer my_client sensor/nhietdo  ← Consumer reconnects
  │
  ├─ MSG_CONNECT → broker_register_session("my_client")
  ├─ MSG_SUBSCRIBE → broker_subscribe(socket, "sensor/nhietdo")
  │
  └─ replay_missed_messages("my_client", "sensor/nhietdo")
       │
       ├─ cursor = get_cursor("my_client", "sensor/nhietdo") → 497
       │   (loaded from cursors.txt)
       │
       ├─ Open data/sensor_nhietdo.log → file_size = 800
       ├─ Since 800 > 497 → there are unread messages
       │
       └─ Replay records from offset 497 to 800
          → Consumer receives all messages published between
            last ACK and the crash
          → ZERO DATA LOSS ✓
```

---

## Wildcard Matching Algorithm Detail

**Function:** `match_topic(sub, pub)` in `broker.c:406`

```
Input:  sub = subscription pattern (may contain + and #)
        pub = published topic name (concrete, no wildcards)
Output: 1 = match, 0 = no match

Algorithm (character by character):
  │
  ├─ If *sub == '#':
  │     return 1   (# matches everything remaining, including nothing)
  │
  ├─ If *sub == '+':
  │     Advance pub past one level: while (*pub != '/' && *pub != '\0') pub++
  │     Advance sub past '+': sub++
  │     Continue loop
  │
  ├─ If *sub == *pub:
  │     If both == '\0': return 1  (complete match)
  │     sub++; pub++
  │     Continue loop
  │
  └─ Else (mismatch):
        Special case: sub=".../#" and pub at '\0'
          → "sport/#" matches "sport" (# = zero levels)
          If *sub=='/' && *(sub+1)=='#' && *(sub+2)=='\0' && *pub=='\0':
            return 1
        Otherwise: return 0

Examples traced through:

  match_topic("sensor/+/temp", "sensor/room1/temp"):
    s='s' p='s' → match → advance
    s='e' p='e' → match → advance
    ... (match through "sensor/")
    s='+' → skip pub past "room1" → pub at '/'
    s='/' p='/' → match → advance
    s='t' p='t' → match → advance
    ... (match through "temp")
    s='\0' p='\0' → return 1 ✓

  match_topic("sensor/+/temp", "sensor/r1/r2/temp"):
    ... (match through "sensor/")
    s='+' → skip pub past "r1" → pub at '/'
    s='/' p='/' → match → advance
    s='t' p='r' → MISMATCH → return 0 ✗

  match_topic("sensor/#", "sensor"):
    ... (match through "sensor")
    s='\0'? No, s='/' p='\0'
    Mismatch: s='/' p='\0'
    Special case check: *s=='/' && *(s+1)=='#' && *(s+2)=='\0' && *p=='\0'
    → return 1 ✓  (# matches zero levels)
```

---

## Lock Ordering and Synchronization Summary

To prevent deadlocks, locks are always acquired in this order:

```
broker.global_lock          ← Acquired FIRST
  └─ topic->topic_lock      ← Acquired SECOND
       └─ pending_ack_lock   ← Acquired THIRD
            └─ storage_mutex  ← Acquired FOURTH
                 └─ cursor_lock  ← Acquired FIFTH
                      └─ msg_id_lock ← Acquired LAST (leaf lock)
```

**Rule:** A thread holding a lower-priority lock must NEVER attempt to acquire a higher-priority lock.

| Mutex | Protects | Used By |
|-------|----------|---------|
| `broker.global_lock` | topics[] array, topic_count | find_or_create_topic, broker_publish, broker_unsubscribe, broker_remove_client |
| `topic->topic_lock` | subscriber_sockets[], sub_count (per-topic) | broker_subscribe, broker_unsubscribe, broker_publish |
| `pending_ack_lock` | PendingAck linked list | broker_publish, broker_handle_ack, redelivery_worker, cleanup_pending_acks |
| `storage_mutex` | File I/O during write and trim | storage_save_message, trim_log_file, replay_missed_messages |
| `cursor_lock` | cursor_records[] array | get_cursor, set_cursor, load_cursors, save_cursors, retention (read min offset) |
| `msg_id_lock` | global_msg_id_counter | get_next_global_msg_id |
| `session_lock` | active_sessions[] array | broker_register_session, broker_remove_session, broker_get_client_id |

---

## Complete Data Flow Summary

```
Producer                    Broker                           Disk
   │                          │                               │
   │── PUBLISH ──────────────►│                               │
   │   (header+topic+payload) │                               │
   │                          │── storage_save_message() ────►│
   │                          │   ftruncate + mmap + memcpy   │
   │                          │   + msync(MS_SYNC)            │
   │                          │◄── return end_offset ─────────│
   │                          │                               │
   │                          │── match_topic() for all subs  │
   │                          │── dedup check                 │
   │                          │── create PendingAck           │
   │                          │                               │
   │                          │── send() to Consumer ────────►│ Consumer
   │                          │                               │    │
   │                          │◄──────────── ACK ─────────────│────┘
   │                          │                               │
   │                          │── set_cursor(offset) ────────►│
   │                          │   save to cursors.txt         │
   │                          │── remove PendingAck           │
   │                          │── free memory                 │
```

```
                         Background Threads
                              │
              ┌───────────────┼───────────────┐
              │                               │
    Redelivery Worker (1s)          Retention Worker (5s)
              │                               │
    Scan PendingAck list            Scan data/*.log files
              │                               │
    If timeout >= 5s:               Compute trim amount:
      retry_count++                   MAX(time, size, acked)
      If > 3: disconnect                      │
      Else: retransmit              trim_log_file()
                                    Adjust cursors
                                    Adjust pending offsets
```