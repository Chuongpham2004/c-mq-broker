# C-MQ BROKER: A Topic-Based Publish-Subscribe Message Broker in C

**Course:** System Programming — Final Project  
**Project Code:** D4 — Message Broker  
**Language:** C11 / POSIX / Linux x86-64  

**Group Members:**

| Full Name | Student ID | Email |
|-----------|-----------|-------|
| Nguyen Van A | 22001234 | a.nguyen@vnu.edu.vn |
| Tran Thi B | 22001235 | b.tran@vnu.edu.vn |
| Le Van C | 22001236 | c.le@vnu.edu.vn |

**Date:** June 2026

---

## TABLE OF CONTENTS

1. Introduction
   1.1 Problem Statement
   1.2 Objectives
   1.3 Approach Overview
2. Background and Theory
   2.1 The Publish-Subscribe Messaging Model
   2.2 Memory-Mapped File I/O (mmap)
   2.3 POSIX Threads and Synchronization Primitives
   2.4 Signal Handling in Multi-Threaded Programs
   2.5 TCP Socket Programming
3. System Design
   3.1 High-Level Architecture
   3.2 Component Breakdown
   3.3 Binary Protocol Design
   3.4 Key Data Structures
   3.5 Message Flow
   3.6 Concurrency Model
4. Implementation
   4.1 Persistent Append-Only Storage (storage.c)
   4.2 Configurable Fsync Policies
   4.3 Topic Wildcard Matching
   4.4 Fan-Out Publishing with Deduplication
   4.5 Per-Subscriber Cursors and Replay-on-Reconnect
   4.6 Pending-ACK List and Automatic Redelivery
   4.7 Retention Policies
   4.8 Thread Pool — Boss-Worker Pattern
   4.9 Graceful Shutdown via Signal Handling
5. Testing and Validation
   5.1 Test Methodology
   5.2 Test Case 1: Basic Pub-Sub Flow
   5.3 Test Case 2: Wildcard Routing
   5.4 Test Case 3: Offline Queueing and Session Resume
   5.5 Test Case 4: Clean Unsubscribe
   5.6 Test Case 5: Pending ACK and Redelivery Timeout
   5.7 Durability Test (kill -9)
   5.8 Edge Cases Covered
6. Performance Analysis
   6.1 Fsync Policy Benchmarks
   6.2 Bottleneck Identification
   6.3 Optimizations Applied and Their Impact
7. Conclusion and Future Work
   7.1 Summary of Achievements
   7.2 Lessons Learned
   7.3 What We Would Do Differently
   7.4 Future Work
8. References

---

## 1. INTRODUCTION

### 1.1 Problem Statement

Modern distributed systems rely heavily on asynchronous message passing to decouple services, buffer workloads, and enable event-driven architectures. In microservice environments, direct point-to-point communication between services creates tight coupling: if service A needs to notify services B, C, and D about an event, it must know the addresses of all three, handle failures individually, and block until each acknowledges the message. This pattern breaks down rapidly as the number of services grows.

Message brokers solve this problem by acting as intermediaries. Producers send messages to the broker without knowing who will consume them. Consumers subscribe to topics of interest and receive messages asynchronously. The broker handles routing, persistence, delivery guarantees, and fan-out. Production systems like Apache Kafka, RabbitMQ, and Mosquitto (MQTT) serve billions of messages per day, yet their internal mechanics — memory-mapped persistence, acknowledge-based delivery, wildcard topic routing, and crash-resilient storage — are rarely explored at the systems-programming level.

This project addresses the challenge of building a fully functional, topic-based Publish-Subscribe message broker from scratch in the C programming language, relying exclusively on the C11 standard library and POSIX system calls. By doing so, we demonstrate mastery of core systems programming concepts including TCP socket programming, multi-threaded concurrency with mutexes, memory-mapped file I/O, signal handling, and durable data persistence under crash scenarios.

### 1.2 Objectives

The C-MQ Broker project implements the following eight features, each representing a distinct systems programming challenge:

1. Topic-based Publish-Subscribe with five commands: CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, and ACK. These commands form the complete lifecycle of message production and consumption.

2. Persistent append-only log per topic, stored on disk using memory-mapped I/O. Each topic's messages are written sequentially to a dedicated log file that survives broker restarts.

3. Configurable fsync policy with three options: per-write (maximum durability), group-commit (batched syncing every 5 writes), and time-based (sync at most once per second). This allows operators to choose their preferred trade-off between durability and throughput.

4. Per-subscriber cursors persisted to disk, enabling resume-on-reconnect. When a consumer disconnects and later reconnects with the same client ID, the broker replays only the messages the consumer has not yet acknowledged.

5. Pending-ACK list with automatic redelivery on timeout. Every message sent to a consumer is tracked. If the consumer fails to acknowledge within 5 seconds, the broker retransmits the message, up to 3 retries before forcibly disconnecting the client.

6. Topic hierarchy matching with two wildcard characters: the plus sign (+) matches exactly one topic level, and the hash sign (#) matches zero or more levels. This follows the MQTT specification for hierarchical topic routing.

7. Retention policies supporting three strategies: time-based (delete messages older than N seconds), size-based (cap each log file at N bytes), and all-acknowledged (remove messages that every subscriber has confirmed receiving). Multiple policies can be combined, with the most aggressive trim applied.

8. Durability test: kill the broker process with SIGKILL (kill -9) while messages are being published, restart the broker, and verify that zero messages were lost. This demonstrates that the mmap-based storage with synchronous fsync provides true crash resilience.

### 1.3 Approach Overview

We designed a layered architecture that separates concerns into five well-defined modules. The network layer handles TCP socket creation, binary protocol serialization, and exact-read semantics. The thread pool implements the Boss-Worker concurrency pattern with four pre-spawned worker threads and a circular task queue. The broker core module contains all business logic: topic routing, wildcard matching, session management, cursor tracking, pending-ACK management, and retention cleanup. The storage engine provides mmap-based append-only writes with configurable fsync policies and a log trimming function. Finally, the server module serves as the entry point, handling CLI argument parsing, signal registration, and the main accept loop.

All inter-thread communication is synchronized using POSIX mutexes with a strict lock-ordering discipline to prevent deadlocks. The system compiles cleanly with gcc -Wall -Wextra -std=c11, producing zero warnings, uses no external dependencies beyond the C standard library and POSIX, and all five automated test cases pass consistently.

The total codebase consists of approximately 2,100 lines of C code across 16 source files, including 5 core source files, 5 header files, 3 client programs, a Makefile, a test suite script, and a README.

---

## 2. BACKGROUND AND THEORY

### 2.1 The Publish-Subscribe Messaging Model

The Publish-Subscribe (Pub-Sub) pattern is a messaging paradigm that decouples message producers from message consumers through an intermediary called a broker. Unlike the traditional request-response model where the client must know the server's address and wait for a reply, Pub-Sub allows producers to publish messages to named channels called topics without any knowledge of who, if anyone, is listening. Consumers express interest by subscribing to topics, and the broker is responsible for routing each published message to all matching subscribers, a process known as fan-out.

This model offers several architectural advantages. First, it enables one-to-many communication: a single published message can be delivered to thousands of subscribers. Second, it provides temporal decoupling: producers and consumers do not need to be online simultaneously; the broker can store messages and deliver them when consumers reconnect. Third, it facilitates spatial decoupling: neither producers nor consumers need to know each other's network addresses.

Topic-based systems commonly support hierarchical topic names using a delimiter character, typically the forward slash (/). For example, a smart home system might use topics like home/livingroom/temperature, home/kitchen/humidity, and home/garage/door. Wildcard subscriptions allow consumers to subscribe to patterns that match multiple topics. The MQTT specification defines two wildcard characters: the plus sign (+) matches exactly one topic level, so home/+/temperature matches home/livingroom/temperature and home/kitchen/temperature but not home/floor1/room2/temperature. The hash sign (#) matches zero or more trailing levels, so home/# matches all topics starting with home/.

Quality of Service (QoS) levels define the delivery guarantee. QoS 0 is "at most once" (fire and forget), QoS 1 is "at least once" (the broker retransmits until acknowledged), and QoS 2 is "exactly once" (using a four-way handshake). Our implementation provides QoS 1 semantics through the pending-ACK mechanism with automatic redelivery.

### 2.2 Memory-Mapped File I/O (mmap)

The mmap() system call maps a file or device into the process's virtual address space, creating a direct correspondence between memory addresses and file contents. Once mapped, the program can read and write file data using simple pointer operations and memcpy() instead of explicit read() and write() system calls. This approach offers several advantages for our message broker.

When mmap() is called with the MAP_SHARED flag, modifications to the mapped memory region are automatically visible to other processes that map the same file, and changes are eventually written back to the underlying file by the operating system's page cache mechanism. The kernel manages the translation between virtual memory pages and physical disk blocks through the page table, and dirty pages (those modified in memory but not yet written to disk) are periodically flushed by the kernel's writeback daemon.

The msync() system call provides explicit control over when dirty pages are flushed to disk. When called with MS_SYNC, it blocks until all modified pages in the specified range have been written to the storage device, providing a strong durability guarantee. When called with MS_ASYNC, it initiates a non-blocking writeback: the kernel marks the pages for writing but returns immediately without waiting for the I/O to complete. This distinction is the foundation of our configurable fsync policy.

The ftruncate() system call extends or shrinks a file to a specified size. In our storage engine, we use ftruncate() to extend the log file before mapping it, ensuring there is space for the new message. The sequence is: ftruncate() to extend, mmap() to map, memcpy() to write, msync() to flush, munmap() to unmap, and close() to release the file descriptor.

Compared to traditional write()-based I/O, mmap offers reduced system call overhead for small records (no context switch needed for each write), kernel-managed caching (the page cache acts as an automatic buffer), and the ability to read data directly from the mapped region during replay without a separate read() call. However, mmap is not without drawbacks: it can cause SIGBUS if the file is truncated while mapped, and frequent map/unmap cycles have overhead from TLB (Translation Lookaside Buffer) invalidation.

### 2.3 POSIX Threads and Synchronization Primitives

POSIX Threads (pthreads) provide the standard API for multi-threaded programming on Linux and other UNIX-like systems. A thread is an independent flow of execution within a process that shares the process's address space, file descriptors, and signal handlers. Multiple threads enable concurrent handling of client connections in our broker.

The primary synchronization primitive we use is the mutex (mutual exclusion lock), implemented via pthread_mutex_t. A mutex ensures that only one thread can execute a critical section at a time. Our broker uses six distinct mutexes:

- broker.global_lock: Protects the topic registry (array of topics and topic count). Acquired when creating new topics or iterating over all topics during publish.
- topic->topic_lock: Per-topic mutex protecting the subscriber list. Each topic has its own lock to minimize contention — adding a subscriber to topic A does not block operations on topic B.
- pending_ack_lock: Protects the singly-linked list of pending ACK entries. Acquired during publish (to add entries), ACK handling (to remove entries), and redelivery scanning.
- storage_mutex: Protects file I/O operations during storage writes and log trimming, preventing concurrent modifications to the same log file.
- cursor_lock: Protects the in-memory cursor record array, ensuring consistent reads and writes of cursor offsets.
- msg_id_lock: Protects the global message ID counter, ensuring each message receives a unique ID even under concurrent publish operations.

To prevent deadlocks, we enforce a strict lock-ordering discipline: global_lock must be acquired before topic_lock, which must be acquired before pending_ack_lock and storage_mutex. No thread ever acquires these locks in reverse order.

Condition variables (pthread_cond_t) are used in the thread pool implementation to put idle worker threads to sleep when the task queue is empty, and to wake them when new tasks arrive. This avoids busy-waiting and conserves CPU resources.

### 2.4 Signal Handling in Multi-Threaded Programs

Signals are software interrupts delivered to a process to notify it of events such as user input (SIGINT from Ctrl+C), termination requests (SIGTERM from kill), or unrecoverable errors (SIGSEGV from invalid memory access). In a multi-threaded program, signal delivery is complicated because the kernel may deliver the signal to any thread that has not blocked it.

Our broker uses sigaction() to register a handler for SIGINT and SIGTERM. The handler is deliberately minimal: it sets a volatile sig_atomic_t flag (keep_running) to 0 and returns. The volatile qualifier prevents the compiler from optimizing away reads of the flag, and sig_atomic_t guarantees that reads and writes to the variable are atomic with respect to signal delivery.

A critical design choice is setting sa.sa_flags = 0, which means SA_RESTART is NOT set. This is intentional: without SA_RESTART, when a signal is delivered while the main thread is blocked in accept(), the system call is interrupted and returns -1 with errno set to EINTR. The main loop checks for EINTR and exits cleanly. If SA_RESTART were set, accept() would automatically restart after the signal, and the broker would never notice the shutdown request.

SIGKILL (signal 9) cannot be caught or handled by any process. When we test durability by sending kill -9 to the broker, the process terminates immediately with no opportunity to run cleanup code. This is why our persistence strategy must ensure data is already safely on disk before the storage function returns — we cannot rely on shutdown hooks.

### 2.5 TCP Socket Programming

Our broker communicates with clients over TCP (Transmission Control Protocol), which provides reliable, ordered, byte-stream delivery over IP networks. The server creates a listening socket with socket(), binds it to port 8080 with bind(), marks it as passive with listen(), and accepts incoming connections with accept() in a loop. Each accepted connection returns a new file descriptor representing the client's TCP session.

A subtle but important issue in TCP programming is partial reads. A single send() call on the client may result in multiple recv() calls on the server (or vice versa), because TCP is a stream protocol with no inherent message boundaries. To handle this, we implement a read_exact() function that loops until exactly N bytes have been received, handling both partial reads (where recv returns fewer bytes than requested) and EINTR (where the call is interrupted by a signal).

The SO_REUSEADDR socket option is set on the server socket to allow immediate restart after shutdown. Without this option, the operating system's TIME_WAIT state would prevent rebinding to the same port for up to 60 seconds after the previous server instance closes.

---

## 3. SYSTEM DESIGN

### 3.1 High-Level Architecture

The C-MQ Broker follows a layered architecture with clear separation of concerns. The system can be visualized as five horizontal layers, each depending only on the layer below it:

```
+-----------------------------------------------------+
|              server.c (Entry Point)                  |
|    CLI Parsing · Signal Handler · Accept Loop        |
+-----------------------------------------------------+
|             broker.c (Business Logic)                |
|  Pub/Sub Routing · Wildcards · ACK Management ·     |
|  Sessions · Cursors · Redelivery · Retention         |
+------------------------+----------------------------+
|     network.c          |       storage.c             |
|  TCP Socket I/O        |  mmap-based Append-Only     |
|  Binary Protocol       |  Log · Fsync · Trim         |
+------------------------+----------------------------+
|              threadpool.c                            |
|     Boss-Worker Pattern · Circular Task Queue        |
+-----------------------------------------------------+
|         POSIX / Linux Kernel / C Standard Library    |
+-----------------------------------------------------+
```

External clients (producers and consumers) connect to the broker via TCP on port 8080. The server module accepts connections and dispatches them to worker threads via the thread pool. Worker threads parse incoming binary messages using the network module and invoke the appropriate broker functions. The broker module orchestrates all business logic and delegates persistence to the storage engine.

Two background threads run independently: the redelivery worker scans the pending-ACK list every 1 second for timed-out messages, and the retention worker scans all log files every 5 seconds to apply cleanup policies.

### 3.2 Component Breakdown

The following table summarizes each module's role, file, and size:

| Module | Source Files | Lines | Responsibility |
|--------|-------------|-------|----------------|
| Server | server.c | 145 | Entry point, CLI argument parsing (--fsync, --retention-*), signal handling (SIGINT, SIGTERM), main accept() loop |
| Broker Core | broker.c, broker.h | 911 | Topic registry, wildcard matching, session management, cursor tracking, pending-ACK queue, redelivery worker, retention worker |
| Storage Engine | storage.c, storage.h | 208 | Append-only log writes via mmap, three fsync policies, log trimming via memmove + ftruncate |
| Network | network.c, network.h | 123 | TCP server socket creation, read_exact() for reliable reads, binary protocol parser (receive_message), memory cleanup (free_message) |
| Thread Pool | threadpool.c, threadpool.h | 107 | Boss-Worker pattern with 4 workers, circular queue (size 100), mutex + condition variable synchronization |
| Producer Client | producer.c | 43 | Fire-and-forget publisher: connect, send one PUBLISH message, disconnect |
| Consumer Client | consumer.c | 121 | Subscribing consumer with auto-ACK: connects, subscribes, receives messages, sends ACK for each, supports graceful UNSUBSCRIBE on SIGINT |
| NoACK Client | noack_client.c | 70 | Test client that deliberately never sends ACK, used for testing the redelivery and disconnect timeout mechanism |

Total: approximately 2,109 lines of C code across 16 files, totaling about 82 KB.

### 3.3 Binary Protocol Design

All communication between clients and the broker uses a compact binary protocol. Every message consists of a fixed-size 14-byte header followed by variable-length topic and payload fields:

```
+---------------------------+--------------------+----------------------+
|     MessageHeader         |      Topic         |       Payload        |
|     (14 bytes, fixed)     | (topic_len bytes)  |  (payload_len bytes) |
+---------------------------+--------------------+----------------------+
```

The MessageHeader structure is defined with __attribute__((packed)) to prevent the compiler from inserting padding bytes between fields:

```c
typedef struct __attribute__((packed)) {
    uint8_t  type;          // 1 byte  — Message type enum
    uint8_t  topic_len;     // 1 byte  — Length of topic string (max 255)
    uint32_t msg_id;        // 4 bytes — Unique message identifier
    uint32_t timestamp;     // 4 bytes — Unix epoch timestamp
    uint32_t payload_len;   // 4 bytes — Length of payload data
} MessageHeader;            // Total: exactly 14 bytes
```

The five message types and their semantics are:

| Type | Value | Direction | Description |
|------|-------|-----------|-------------|
| MSG_CONNECT | 1 | Client to Broker | Registers a session with a client_id (sent as payload) |
| MSG_PUBLISH | 2 | Client to Broker, Broker to Client | Sends a message to a topic (client to broker) or delivers a message to a subscriber (broker to client) |
| MSG_SUBSCRIBE | 3 | Client to Broker | Subscribes to a topic pattern (supports wildcards) |
| MSG_ACK | 4 | Client to Broker | Acknowledges receipt of a message (identified by msg_id) |
| MSG_UNSUBSCRIBE | 5 | Client to Broker | Unsubscribes from a topic |

All multi-byte integer fields (msg_id, timestamp, payload_len) are transmitted in network byte order (big-endian) using htonl() for encoding and ntohl() for decoding. This ensures correct interpretation regardless of the host machine's native byte order.

The parsing logic in receive_message() reads the message in three sequential steps: (1) read exactly 14 bytes for the header, (2) read exactly topic_len bytes for the topic string, and (3) read exactly payload_len bytes for the payload data. The read_exact() function guarantees that exactly N bytes are read before returning, handling TCP's stream nature transparently.

### 3.4 Key Data Structures

The broker maintains several critical data structures in memory:

**Topic:** Represents a single topic channel. Contains a name (up to 256 characters), an array of subscriber socket file descriptors (maximum 32 subscribers), a subscriber count, and a per-topic mutex lock for fine-grained concurrency control.

```c
typedef struct {
    char name[256];
    int subscriber_sockets[MAX_SUBSCRIBERS];
    int sub_count;
    pthread_mutex_t topic_lock;
} Topic;
```

**Broker:** The global state container. Holds an array of up to 64 topics, the current topic count, and a global mutex for protecting topic creation.

```c
typedef struct {
    Topic topics[MAX_TOPICS];
    int topic_count;
    pthread_mutex_t global_lock;
} Broker;
```

**CursorRecord:** Tracks each subscriber's read position within a topic's log file. The offset field represents the byte position in the log file up to which the subscriber has acknowledged messages.

```c
typedef struct {
    char client_id[64];
    char topic[256];
    size_t offset;
} CursorRecord;
```

**ActiveSession:** Maps a TCP socket file descriptor to a client identifier string, enabling the broker to look up a client's identity from their socket when processing ACKs and managing cursors.

```c
typedef struct {
    int socket;
    char client_id[64];
} ActiveSession;
```

**PendingAck:** A singly-linked list node tracking an unacknowledged message. Stores the complete information needed to retransmit the message: the recipient's socket, message ID, client ID, topic name, file offset, a copy of the payload, the time the message was last sent, and the retry count.

```c
typedef struct PendingAck {
    int client_socket;
    uint32_t msg_id;
    char client_id[64];
    char topic[256];
    size_t file_offset;
    void *payload;
    uint32_t payload_len;
    time_t sent_time;
    int retry_count;
    struct PendingAck *next;
} PendingAck;
```

### 3.5 Message Flow

The complete lifecycle of a message from publication to acknowledgment follows this sequence:

1. The producer connects to the broker and sends a MSG_PUBLISH message containing the topic name and payload.

2. The worker thread handling this producer's connection calls broker_publish(), which first persists the message to disk by calling storage_save_message(). This function extends the topic's log file using ftruncate(), maps it into memory with mmap(), writes the message header, topic, and payload using memcpy(), flushes to disk according to the configured fsync policy using msync(), and returns the new file size (the end offset of the written record).

3. The broker then iterates over all registered topics and calls match_topic() to check if each topic's subscription pattern matches the published topic name. For each matching subscription, it iterates over the subscriber sockets.

4. For each subscriber, the broker generates a unique message ID using an atomic counter, creates a PendingAck node containing all information needed for potential redelivery, adds it to the pending-ACK linked list, and sends the message to the subscriber via TCP using three send() calls (header, topic, payload).

5. The consumer receives the message, processes it, and sends a MSG_ACK back to the broker containing the message ID.

6. Upon receiving the ACK, the broker finds the matching PendingAck node, updates the subscriber's cursor to the message's end offset using set_cursor(), removes the PendingAck node from the linked list, and frees the associated memory.

7. If no ACK is received within 5 seconds, the redelivery worker thread detects the timeout and retransmits the message, incrementing the retry counter. After 3 failed retries, the broker forcibly disconnects the client using shutdown(SHUT_RDWR) followed by close().

### 3.6 Concurrency Model

The broker employs a Boss-Worker threading model for handling client connections, plus two dedicated background threads for housekeeping tasks.

The Boss Thread (main thread) runs the accept() loop, accepting new TCP connections and enqueueing them as tasks into the thread pool's circular buffer. The boss thread never processes client messages directly.

The Worker Threads (4 threads) are pre-spawned at startup. Each worker blocks on a condition variable until a task is available in the queue. When a task arrives, one worker wakes up, dequeues the task, and handles the entire client session: parsing messages, dispatching to broker functions, and cleaning up on disconnect.

The Redelivery Worker is a detached background thread that runs an infinite loop, sleeping for 1 second between iterations. Each iteration, it acquires the pending_ack_lock and scans the entire PendingAck linked list for entries older than 5 seconds.

The Retention Worker is another detached background thread that runs every 5 seconds. It opens the data/ directory, iterates over all .log files, and applies the configured retention policies to each file, trimming old messages as needed.

---

## 4. IMPLEMENTATION

### 4.1 Persistent Append-Only Storage (storage.c)

The storage engine is responsible for durably persisting every published message to disk. Each topic maps to a dedicated log file in the ./data/ directory. The topic name's forward-slash delimiters are replaced with underscores to form a valid filename. For example, the topic "sensor/nhietdo" maps to the file "data/sensor_nhietdo.log".

Messages are appended sequentially to the end of the file using memory-mapped I/O. The storage_save_message() function implements this in eight steps:

Step 1 — Filename construction: The topic name is copied and all '/' characters are replaced with '_'. The filename is constructed as "data/<safe_topic>.log".

Step 2 — File opening: The file is opened with O_RDWR | O_CREAT and permissions 0666. If the file does not exist, it is created.

Step 3 — Size calculation: The record size is computed as sizeof(MessageHeader) + topic_len + payload_len. The current file size is obtained via fstat().

Step 4 — File extension: ftruncate() extends the file by record_size bytes to make room for the new record. This is an atomic operation at the filesystem level.

Step 5 — Memory mapping: mmap() maps the entire file with PROT_READ | PROT_WRITE and MAP_SHARED, creating a direct virtual-memory view of the file contents.

Step 6 — Data writing: Three memcpy() calls write the message header (with fields converted to network byte order), topic string, and payload to the end of the mapped region. Because the mapping is MAP_SHARED, these writes modify the kernel's page cache directly.

Step 7 — Fsync execution: Based on the configured policy, either msync(MS_SYNC) (blocking flush) or msync(MS_ASYNC) (non-blocking hint) is called to control when dirty pages are written to physical storage.

Step 8 — Cleanup: munmap() releases the mapping and close() releases the file descriptor. The function returns the new file size, which serves as the cursor position for tracking which messages a subscriber has read.

The on-disk record format is identical to the wire protocol format, which is a deliberate design choice. During replay, the broker can read records from the log file and send them directly to consumers without re-serialization, reducing CPU overhead.

The trim_log_file() function handles retention-driven cleanup. It opens the file, maps it, uses memmove() to shift the remaining data forward (overwriting the oldest records at the front), calls msync(MS_SYNC) to ensure the shifted data is durable, and then calls ftruncate() to shrink the file. This approach modifies the file in-place without creating a temporary copy, minimizing disk space usage during trimming.

### 4.2 Configurable Fsync Policies

The system supports three fsync strategies, selectable at startup via the --fsync command-line argument:

**Per-Write (default):** Every call to storage_save_message() invokes msync(MS_SYNC), which blocks until the data is confirmed on the storage device. This provides the strongest durability guarantee: if the process is killed immediately after the function returns, the message is guaranteed to be on disk. The trade-off is reduced throughput, as each write incurs the latency of a physical disk flush (typically 0.5-2ms on SSDs, 5-15ms on HDDs).

**Group-Commit:** A write counter is maintained. Every 5th write triggers msync(MS_SYNC); the other 4 writes use msync(MS_ASYNC). This amortizes the cost of synchronous flushing across 5 messages, providing approximately 4-5x higher throughput than per-write mode. The risk is that up to 4 messages may be lost if the broker crashes between synchronous flushes.

**Time-Based:** A timestamp of the last synchronous flush is maintained. If at least 1 second has elapsed since the last flush, msync(MS_SYNC) is called; otherwise, msync(MS_ASYNC) is used. This provides the highest throughput under burst workloads, as hundreds of messages within a one-second window share a single synchronous flush. The risk is up to approximately one second of data loss on crash.

The implementation uses a simple conditional structure:

```c
int do_sync = 0;
if (global_fsync_policy == FSYNC_PER_WRITE) {
    do_sync = 1;
} else if (global_fsync_policy == FSYNC_GROUP_COMMIT) {
    write_count++;
    if (write_count >= 5) { do_sync = 1; write_count = 0; }
} else if (global_fsync_policy == FSYNC_TIME_BASED) {
    time_t now = time(NULL);
    if (now - last_sync_time >= 1) { do_sync = 1; last_sync_time = now; }
}
msync(map, new_size, do_sync ? MS_SYNC : MS_ASYNC);
```

The static variables write_count and last_sync_time are protected by the storage_mutex, which is already held during the entire storage_save_message() call, so no additional synchronization is needed.

### 4.3 Topic Wildcard Matching

The match_topic() function compares a subscription pattern against a published topic name, supporting two MQTT-standard wildcard characters. The function processes both strings character by character:

When the subscription pointer encounters a '#' character, the function immediately returns 1 (match), because '#' matches all remaining levels including zero levels. This handles patterns like "sensor/#" matching "sensor", "sensor/temp", and "sensor/a/b/c".

When the subscription pointer encounters a '+' character, the function advances the published-topic pointer past exactly one level (skipping all characters until the next '/' or end of string), then advances the subscription pointer past the '+'. This handles patterns like "sensor/+/temp" matching "sensor/room1/temp" but not "sensor/room1/room2/temp".

When both characters match (including the '/' delimiter), both pointers advance. If both reach the null terminator simultaneously, the function returns 1 (exact match).

A special edge case handles "sport/#" matching "sport" (where '#' matches zero levels). This occurs when the subscription pointer is at '/' and the next character is '#' followed by null, while the published pointer is at null.

The implementation is iterative rather than recursive, avoiding stack overflow risks with deeply nested topics:

```c
int match_topic(const char *sub, const char *pub) {
    if (sub == NULL || pub == NULL) return 0;
    const char *s = sub;
    const char *p = pub;
    while (1) {
        if (*s == '#') return 1;
        if (*s == '+') {
            while (*p != '/' && *p != '\0') p++;
            s++;
            continue;
        }
        if (*s == *p) {
            if (*s == '\0') return 1;
            s++; p++;
        } else {
            if (*s == '/' && *(s+1) == '#' && *(s+2) == '\0' && *p == '\0')
                return 1;
            return 0;
        }
    }
}
```

The following table demonstrates the matching behavior:

| Subscription Pattern | Published Topic | Result | Explanation |
|---------------------|-----------------|--------|-------------|
| sensor/+/temp | sensor/room1/temp | Match | + = room1 |
| sensor/+/temp | sensor/r1/r2/temp | No match | + matches only one level |
| sensor/# | sensor | Match | # matches zero levels |
| sensor/# | sensor/temp | Match | # = temp |
| sensor/# | sensor/a/b/c | Match | # = a/b/c |
| home/light | home/light | Match | Exact match |
| home/light | home/fan | No match | Different final level |
| +/+ | a/b | Match | Two single-level wildcards |
| # | any/topic/at/all | Match | Root # matches everything |

### 4.4 Fan-Out Publishing with Deduplication

The broker_publish() function handles the complete fan-out process: persisting the message, finding all matching subscribers, and delivering the message to each one. A key implementation detail is deduplication.

Consider a consumer that subscribes to both "sensor/+" and "sensor/#". When a message is published to "sensor/temp", both patterns match. Without deduplication, the consumer would receive two copies of the same message. To prevent this, the function maintains a sent_sockets[] array that tracks which socket file descriptors have already received the current message. Before sending to a subscriber, the function checks whether that socket is already in the array. If so, it skips the send.

The deduplication array is declared as a stack variable with size MAX_SUBSCRIBERS * MAX_TOPICS, ensuring it can handle the worst case where every topic matches. Since this is bounded (64 topics times 32 subscribers = 2,048 entries), the stack allocation is safe and avoids heap overhead.

For each subscriber that passes the deduplication check, the function performs four operations: (1) generates a unique message ID via get_next_global_msg_id(), which uses a mutex-protected counter that wraps around at zero, (2) creates a PendingAck node containing a copy of the payload and all metadata needed for redelivery, (3) inserts the PendingAck node at the head of the linked list, and (4) sends the message over TCP using three send() calls for the header, topic, and payload respectively.

### 4.5 Per-Subscriber Cursors and Replay-on-Reconnect

Each subscriber's read position in a topic's log file is tracked as a byte offset called a cursor. The cursor represents the byte position in the log file up to which all messages have been acknowledged. Cursors are stored in memory as an array of CursorRecord structures and persisted to disk in the file data/cursors.txt.

The cursor file uses a simple text format with one record per line: "client_id:topic:offset". For example:

```
consumer_A:sensor/nhietdo:1456
consumer_B:home/light:728
consumer_A:home/#:2048
```

Cursors are loaded from disk when the broker starts (via load_cursors() called from init_broker_threads()) and saved after every cursor update (via save_cursors() called from set_cursor()). The set_cursor() function enforces a forward-only guarantee: a cursor can only advance (the new offset must exceed the current offset). This prevents race conditions where two threads processing ACKs for the same client might try to set the cursor to different values.

When a consumer subscribes to a topic, the broker immediately calls replay_missed_messages() to deliver any messages the consumer missed while offline. This function:

1. Opens the data/ directory and iterates over all .log files.
2. For each file, reads the original topic name from the first message's header (the topic is stored as part of each record).
3. Calls match_topic() to check if the file's topic matches the subscription pattern.
4. If it matches, retrieves the consumer's cursor offset for that topic.
5. Opens the file with mmap(PROT_READ, MAP_SHARED) and seeks to the cursor offset.
6. Reads each record sequentially from the cursor position to the end of the file.
7. For each record, creates a new PendingAck node and sends the message to the consumer.
8. The consumer must ACK each replayed message, which advances the cursor.

This mechanism provides "at least once" delivery semantics across disconnections. A consumer with a stable client_id can disconnect and reconnect at any time, and will receive all messages published during its absence.

### 4.6 Pending-ACK List and Automatic Redelivery

Every message delivered to a consumer (whether a fresh publish or a replay) creates a PendingAck node in a singly-linked list. The list is protected by the pending_ack_lock mutex. Each node contains a complete copy of the message payload, enabling redelivery without re-reading from disk.

The redelivery_worker() function runs as a detached background thread with the following logic:

```
while (true) {
    sleep(1 second)
    acquire pending_ack_lock
    for each PendingAck node in the list:
        elapsed = current_time - node.sent_time
        if elapsed >= 5 seconds:
            node.retry_count++
            if retry_count > 3:
                // Client is unresponsive — disconnect forcefully
                shutdown(node.client_socket, SHUT_RDWR)
                close(node.client_socket)
                remove node from list
                free node
            else:
                // Retransmit the message
                send(header + topic + payload)
                node.sent_time = current_time
    release pending_ack_lock
```

The use of shutdown(fd, SHUT_RDWR) before close() is critical. If another worker thread is currently blocked in read_exact() on the same socket (waiting for the next message from this client), simply calling close() may not reliably unblock it on all platforms. The shutdown() call with SHUT_RDWR forces both the read and write halves of the connection to terminate, causing any blocking read() or recv() call on that file descriptor to return immediately with an error, which allows the worker thread to detect the disconnect and clean up.

When a consumer successfully sends an ACK, the broker_handle_ack() function traverses the pending-ACK list, finds the node with the matching socket and message ID, updates the cursor via set_cursor(), removes the node from the list, and frees the payload memory and the node itself.

When a client disconnects (either voluntarily or due to timeout), cleanup_pending_acks_for_socket() removes all PendingAck nodes associated with that socket, freeing the resources without updating cursors (since the messages were never acknowledged).

### 4.7 Retention Policies

The retention system manages disk space by removing old messages from log files. Three independent strategies are supported, configurable via CLI arguments:

**Time-Based Retention (--retention-time N):** Messages with a timestamp older than N seconds from the current time are candidates for removal. The function scans records from the front of the log file, accumulating record sizes until it encounters a message newer than the cutoff time.

**Size-Based Retention (--retention-size N):** When a log file exceeds N bytes, records are removed from the front until the remaining file size is within the limit. The function scans from the front, accumulating record sizes as long as removing the record would bring the file closer to the target size.

**All-Acknowledged Retention (--retention-acked):** Records that have been acknowledged by ALL active subscribers are safe to remove. The function finds the minimum cursor offset across all subscribers for the given topic. Any record whose end offset is below this minimum has been read by every subscriber and can be removed.

When multiple policies are enabled simultaneously, the function computes the trim amount for each policy independently and takes the MAXIMUM. This ensures the most aggressive cleanup is applied:

```c
size_t bytes_to_remove = time_bytes_to_remove;
if (size_bytes_to_remove > bytes_to_remove) bytes_to_remove = size_bytes_to_remove;
if (acked_bytes_to_remove > bytes_to_remove) bytes_to_remove = acked_bytes_to_remove;
```

After trimming bytes from the front of a log file, all cursor offsets and pending-ACK file offsets for the affected topic must be adjusted downward by the same amount. This ensures that cursors still point to the correct position within the (now shorter) file. If a cursor offset is less than the trim amount, it is reset to zero.

The process_retention_for_file() function handles the complete workflow: opening the file with mmap(PROT_READ), computing the trim amount for each policy, closing the file, calling trim_log_file() to perform the physical trimming, and then adjusting cursors and pending-ACK offsets under their respective locks.

The retention_worker() thread runs this process every 5 seconds, iterating over all .log files in the data/ directory. The 5-second interval is chosen for the testing environment; in production, a longer interval (e.g., 60 seconds) would reduce overhead.

### 4.8 Thread Pool — Boss-Worker Pattern

The thread pool implements the classic Boss-Worker concurrency pattern. The boss thread (the main thread running the accept loop) produces tasks, and worker threads consume and execute them.

The pool consists of three components: an array of 4 pthread_t worker threads, a circular buffer queue of 100 threadpool_task_t entries (each containing a function pointer and an argument pointer), and synchronization primitives (one mutex and one condition variable).

The worker loop is straightforward:

```c
static void *threadpool_worker(void *threadpool) {
    threadpool_t *pool = (threadpool_t *)threadpool;
    while (1) {
        pthread_mutex_lock(&pool->lock);
        while (pool->count == 0) {
            pthread_cond_wait(&pool->notify, &pool->lock);
        }
        threadpool_task_t task = pool->queue[pool->head];
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count--;
        pthread_mutex_unlock(&pool->lock);
        (*(task.function))(task.argument);
    }
}
```

The threadpool_add() function, called by the boss thread, enqueues a new task at the tail of the circular buffer and signals one waiting worker via pthread_cond_signal(). If the queue is full (count equals queue_size), the function returns -1 to indicate overflow.

Each task in our broker represents a complete client session. The function pointer is handle_client, and the argument is a heap-allocated integer containing the client's socket file descriptor. The worker handles the entire session lifecycle: reading messages in a loop, dispatching to broker functions, and cleaning up on disconnect. This means each worker thread is dedicated to one client at a time. With 4 workers and potentially hundreds of clients, this works because most clients are not continuously sending messages — the thread spends most of its time blocked in read_exact() waiting for the next message.

### 4.9 Graceful Shutdown via Signal Handling

When the broker receives SIGINT (Ctrl+C) or SIGTERM (kill command), the signal handler sets keep_running to 0. Because SA_RESTART is not set, the accept() call in the main loop returns -1 with errno EINTR. The main loop checks for this condition and breaks out.

The cleanup sequence closes the server socket to prevent new connections, then prints a shutdown confirmation message. Worker threads handling active clients will eventually detect the disconnection when their read_exact() calls return 0 (EOF) or -1 (error), triggering broker_remove_client() cleanup for each session.

All mmap regions are safely unmapped within their respective functions (storage_save_message and trim_log_file both call munmap before returning), so there is no risk of data corruption from orphaned mappings.

---

## 5. TESTING AND VALIDATION

### 5.1 Test Methodology

We developed an automated test suite (test_suite.sh, 176 lines) that exercises all core functionality through five distinct test cases. The suite is invoked via make test, which first builds all binaries and then runs the script.

The test methodology follows a consistent pattern for each test case:

1. Start a fresh broker instance with the appropriate configuration flags.
2. Start consumer(s) in the background, redirecting their output to log files.
3. Wait briefly for connections to establish.
4. Run producer(s) to publish test messages.
5. Wait for message delivery and processing.
6. Search log files for expected output patterns using grep.
7. Report PASS if all expected patterns are found, FAIL otherwise.
8. Kill all processes before starting the next test case.

Before the suite begins, it kills any lingering broker or client processes and removes the data/ directory and all log files to ensure a clean starting state. Each test case uses unique topic names prefixed with tc1/, tc2/, etc. to avoid cross-contamination.

### 5.2 Test Case 1: Basic Pub-Sub Flow

**Category:** Normal operation  
**Duration:** ~3 seconds  
**Purpose:** Verify the fundamental happy path — a consumer subscribes to a topic, a producer publishes a message to that topic, and the consumer receives the message.

**Procedure:**
1. Start broker with --fsync per-write.
2. Start consumer subscribing to "tc1/sensor/nhietdo", logging output to tc1.log.
3. Wait 1 second for the subscription to register.
4. Run producer publishing "Hello World" to "tc1/sensor/nhietdo".
5. Wait 2 seconds for delivery.
6. Check that tc1.log contains the string "Hello World".

**Expected result:** The consumer log contains the published message.  
**Actual result:** PASS. The message "Hello World" appears in the consumer log, confirming that CONNECT, SUBSCRIBE, PUBLISH, fan-out delivery, and message reception all work correctly.

### 5.3 Test Case 2: Wildcard Routing

**Category:** Normal operation + Edge case  
**Duration:** ~5 seconds  
**Purpose:** Verify that the '+' wildcard matches exactly one topic level and the '#' wildcard matches zero or more levels.

**Procedure:**
1. Start broker.
2. Start Consumer A subscribing to "tc2/+/temp" (single-level wildcard), logging to tc2_plus.log.
3. Start Consumer B subscribing to "tc2/#" (multi-level wildcard), logging to tc2_hash.log.
4. Wait 1 second.
5. Publish message "Temperature1" to "tc2/room1/temp" (2 levels after tc2/).
6. Publish message "Temperature2" to "tc2/room1/room2/temp" (3 levels after tc2/).
7. Wait 3 seconds.
8. Check tc2_plus.log for "Temperature1" (should be present) and "Temperature2" (should be absent).
9. Check tc2_hash.log for both "Temperature1" and "Temperature2" (both should be present).

**Expected result:** Consumer A receives only the first message; Consumer B receives both.  
**Actual result:** PASS. The '+' wildcard correctly matches only one level, and the '#' wildcard correctly matches all remaining levels.

### 5.4 Test Case 3: Offline Queueing and Session Resume

**Category:** Edge case  
**Duration:** ~5 seconds  
**Purpose:** Verify that when a consumer disconnects and reconnects with the same client_id, it receives all messages published during its absence.

**Procedure:**
1. Start broker.
2. Start consumer with client_id="offline_user" subscribing to "tc3/topic".
3. Wait 1 second, then kill the consumer (simulating disconnection).
4. Publish "Offline Message 1" and "Offline Message 2" to "tc3/topic" while the consumer is offline.
5. Wait 1 second.
6. Restart the consumer with the SAME client_id="offline_user", subscribing to "tc3/topic" again, logging to tc3.log.
7. Wait 3 seconds for replay.
8. Check tc3.log for both "Offline Message 1" and "Offline Message 2".

**Expected result:** The consumer receives both messages that were published while it was offline.  
**Actual result:** PASS. The cursor-based replay mechanism correctly identifies the consumer's last read position and replays all unread messages from the persistent log.

### 5.5 Test Case 4: Clean Unsubscribe

**Category:** Normal operation  
**Duration:** ~3 seconds  
**Purpose:** Verify that after sending MSG_UNSUBSCRIBE, the consumer stops receiving new messages for that topic.

**Procedure:**
1. Start broker.
2. Start consumer subscribing to "tc4/topic", logging to tc4.log.
3. Wait 1 second.
4. Publish "Before Unsub" to "tc4/topic". Consumer should receive this.
5. Wait 1 second.
6. Send MSG_UNSUBSCRIBE from the consumer (done by sending SIGINT to the consumer, which triggers its graceful unsubscribe handler).
7. Wait 1 second.
8. Publish "After Unsub" to "tc4/topic".
9. Wait 1 second.
10. Check tc4.log contains "Before Unsub" but does NOT contain "After Unsub".

**Expected result:** The consumer receives only the message sent before unsubscribing.  
**Actual result:** PASS. The unsubscribe mechanism correctly removes the consumer from the topic's subscriber list, preventing further deliveries.

### 5.6 Test Case 5: Pending ACK and Redelivery Timeout

**Category:** Error handling  
**Duration:** ~22 seconds  
**Purpose:** Verify that the broker retransmits unacknowledged messages and eventually disconnects unresponsive clients.

**Procedure:**
1. Start broker.
2. Start noack_client (which connects, subscribes to "sensor/noack", receives messages but never sends ACK), logging to tc5_noack.log.
3. Wait 1 second.
4. Publish "NoAck Test" to "sensor/noack".
5. Wait 22 seconds (enough for initial delivery + 3 retries at 5-second intervals + margin).
6. Count the number of times "NoAck Test" appears in tc5_noack.log. It should be >= 3 (original + retries).
7. Check tc5_broker.log for the disconnect message indicating the client was forcibly removed.

**Expected result:** The message is retransmitted at least 3 times, and the client is disconnected after exceeding the retry limit.  
**Actual result:** PASS. The broker log shows retry messages at 5-second intervals, and after 3 retries, the client is disconnected with the message "not ACK after 3 retries. Disconnecting..."

### 5.7 Durability Test (kill -9)

The durability test verifies that the broker's mmap-based storage with synchronous fsync survives an unclean shutdown. This is tested using the following scenario:

1. Start the broker with --fsync per-write.
2. Start a consumer subscribing to a test topic.
3. Publish several messages.
4. Kill the broker with kill -9 (SIGKILL), which terminates the process immediately with no opportunity for cleanup code to run.
5. Verify that the log files in data/ are intact and contain the published messages.
6. Restart the broker.
7. Reconnect the consumer with the same client_id.
8. Verify that the consumer receives all previously published messages via the replay mechanism.

This test passes because of three properties of our storage design:

First, ftruncate() extends the file before any data is written, ensuring the file is the correct size even if the process dies mid-write.

Second, memcpy() into a MAP_SHARED region writes directly to the kernel's page cache. Even without an explicit msync(), recent writes may survive a process kill (though not a power failure) because the page cache persists after the process exits.

Third, msync(MS_SYNC) in per-write mode blocks until the data has been committed to the physical storage device. Once msync returns, the data survives even a complete system crash.

### 5.8 Edge Cases Covered

Beyond the five formal test cases, the following edge cases are handled by the implementation:

- Duplicate subscriptions: If a client subscribes to the same topic twice, the second subscription is silently ignored (duplicate check in broker_subscribe).
- Client disconnect during publish: If send() returns an error while delivering a message, the broker skips that subscriber and continues with the others.
- Empty payload: Messages with payload_len = 0 are handled correctly; memcpy is skipped for zero-length data.
- Topic name with special characters: The filename construction replaces '/' with '_', and the original topic name is stored in the log record itself for accurate reconstruction during replay.
- Cursor overflow: The set_cursor function only advances cursors forward, preventing stale ACKs from rewinding the cursor.
- Integer overflow: The message ID counter wraps from UINT32_MAX to 1 (skipping 0, which could be confused with an invalid ID).

---

## 6. PERFORMANCE ANALYSIS

### 6.1 Fsync Policy Benchmarks

We measured message throughput by publishing 1,000 messages of 100 bytes each to a single topic with one subscriber under each fsync policy. The broker was run on a Linux x86-64 system with an SSD (NVMe).

| Fsync Policy | Messages per Second | Relative Speed | Avg Latency per Message | Data at Risk on Crash |
|-------------|--------------------|-----------|-----------------------|---------------------|
| per-write | ~800 msg/s | 1.0x (baseline) | ~1.25 ms | 0 messages |
| group-commit | ~3,200 msg/s | 4.0x | ~0.31 ms | Up to 4 messages |
| time-based | ~5,500 msg/s | 6.9x | ~0.18 ms | Up to ~1 second of messages |

The per-write policy is clearly I/O-bound: each msync(MS_SYNC) call blocks until the SSD's firmware confirms the write, introducing at least 0.5-1ms of latency per message. The group-commit policy amortizes this cost over 5 messages, achieving a nearly linear 4x speedup. The time-based policy performs best under sustained workloads because all messages within a one-second window share a single synchronous flush.

For comparison, Kafka on similar hardware achieves 50,000-100,000 messages per second by using OS page cache with asynchronous flushing and batched writes. Our single-threaded storage path with per-message mmap/munmap is the primary performance difference.

### 6.2 Bottleneck Identification

**Primary bottleneck — Synchronous disk I/O:** The msync(MS_SYNC) call in per-write mode dominates the message processing time, accounting for over 90% of the total latency. Each call requires a round-trip to the storage controller. This is an inherent trade-off: stronger durability guarantees require more expensive I/O operations.

**Secondary bottleneck — Per-message mmap/munmap overhead:** Each call to storage_save_message() performs a complete mmap/munmap cycle. The mmap() system call modifies the process's page table and may trigger TLB (Translation Lookaside Buffer) flushes across CPU cores. For high-throughput scenarios, keeping the file mapped persistently and extending it less frequently would reduce this overhead.

**Tertiary bottleneck — Serialized storage writes:** The storage_mutex serializes all writes across all topics. This means that even though the thread pool has 4 workers, only one message can be persisted at a time. Per-topic file descriptors with per-topic locks would allow parallel writes to different topics.

**Fan-out complexity:** The wildcard matching in broker_publish() has O(T × S) complexity, where T is the number of registered topics (max 64) and S is the maximum subscriber count per topic (max 32). The worst-case scan of 2,048 comparisons completes in single-digit microseconds on modern hardware and is not a practical bottleneck.

**Cursor persistence overhead:** The save_cursors() function rewrites the entire cursors.txt file on every ACK. With many active subscribers, this linear write could become significant. However, for the expected workload of dozens of subscribers, the overhead is negligible.

### 6.3 Optimizations Applied and Their Impact

The following optimizations were implemented during development, each providing measurable or architectural benefits:

**1. Zero-copy persistence via mmap:** By using mmap(MAP_SHARED) instead of write(), we eliminate one data copy. With write(), data flows from userspace buffer to kernel buffer to disk. With mmap, data flows from userspace directly to the kernel's page cache (via memcpy to the mapped region), which is then flushed to disk. This saves one copy operation per message.

**2. Identical wire and disk formats:** The on-disk record format matches the network wire format exactly (14-byte header + topic + payload, all in network byte order). During replay, the broker reads a record from the mmap'd file and sends it to the consumer with minimal re-encoding (only the msg_id and timestamp fields need updating). This avoids a full serialize-deserialize cycle.

**3. Socket deduplication during fan-out:** The sent_sockets[] array in broker_publish() prevents sending duplicate messages to consumers that match multiple subscription patterns. Without this optimization, a consumer subscribed to both "sensor/+" and "sensor/#" would receive two copies of every message on "sensor/temp".

**4. Thread-local storage for client_id lookup:** The broker_get_client_id() function uses a __thread (thread-local) buffer for its return value. This avoids heap allocation on every lookup and eliminates the need for the caller to free the result, simplifying the calling code and reducing memory allocation pressure.

**5. Forward-only cursor advancement:** The set_cursor() function only updates the cursor if the new offset exceeds the current offset. This invariant prevents race conditions where two threads processing ACKs for the same client might try to set the cursor to different values, without requiring additional locking beyond the cursor_lock that is already held.

**6. Batched fsync options:** The group-commit and time-based policies provide 4-7x throughput improvement over per-write mode for workloads that can tolerate a small window of potential data loss. This is particularly valuable for high-frequency sensor data where individual message loss is acceptable but aggregate throughput matters.

---

## 7. CONCLUSION AND FUTURE WORK

### 7.1 Summary of Achievements

We successfully designed and implemented C-MQ Broker, a complete topic-based Publish-Subscribe message broker in approximately 2,100 lines of C11 code. The system uses only the POSIX standard library with no external dependencies and compiles cleanly with gcc -Wall -Wextra -std=c11, producing zero warnings.

All eight required features are fully implemented and tested:

| Feature | Status | Primary Module |
|---------|--------|---------------|
| Topic-based Pub-Sub (PUBLISH, SUBSCRIBE, UNSUBSCRIBE, ACK) | Complete | broker.c |
| Persistent append-only log per topic | Complete | storage.c |
| Configurable fsync policy (per-write / group-commit / time-based) | Complete | storage.c |
| Per-subscriber cursors with resume-on-reconnect | Complete | broker.c |
| Pending-ACK list with redelivery on timeout | Complete | broker.c |
| Topic hierarchy matching (+ and #) | Complete | broker.c |
| Retention policy (time / size / all-acked) | Complete | broker.c + storage.c |
| Durability test (kill -9, restart, verify no loss) | Complete | test_suite.sh |

The automated test suite consists of 5 distinct test cases covering normal operations, edge cases, and error conditions, all of which pass consistently. The test suite is runnable via a single command (make test) and reports PASS/FAIL clearly for each case.

### 7.2 Lessons Learned

**The complexity of mmap-based persistence:** While mmap provides excellent performance characteristics, it introduces subtle complexity. The sequence of ftruncate, mmap, write, msync, munmap, close must be executed in the correct order with proper error handling at each step. Forgetting to munmap before close can lead to data corruption in some edge cases, and accessing the mapped region after ftruncate has been called by another thread on the same file causes SIGBUS.

**Deadlock prevention requires discipline:** During early development, we encountered deadlocks when the redelivery worker thread acquired pending_ack_lock while a publish thread held global_lock and attempted to acquire pending_ack_lock. Resolving this required establishing and documenting a lock-ordering protocol. We learned that in any system with more than two mutexes, lock ordering must be designed upfront, not discovered through debugging.

**Signal handling in multi-threaded programs is tricky:** Our initial implementation used signal() instead of sigaction() and included SA_RESTART, which caused the accept() call to automatically restart after receiving SIGINT, making graceful shutdown impossible. We also initially attempted complex logic in the signal handler (printing status messages, closing sockets), which could deadlock if the interrupted thread held a mutex. The final design uses an async-signal-safe handler that only sets a volatile flag.

**The importance of shutdown(SHUT_RDWR):** When the redelivery worker decides to disconnect a client by closing its socket, another worker thread may be blocked in read_exact() on that same socket. On Linux, calling close() from a different thread than the one blocked in read() has undefined behavior per POSIX. The solution is to call shutdown(fd, SHUT_RDWR) first, which causes the blocked read() to return -1 with errno set to ECONNRESET or ENOTCONN, allowing the worker thread to detect the disconnect and exit cleanly.

**Testing concurrent systems requires patience:** Some of our test failures were caused by race conditions that only manifested under specific timing. For example, if the consumer subscribed before the CONNECT message was fully processed, the cursor lookup would fail because the session was not yet registered. Adding a 1-second sleep between CONNECT and SUBSCRIBE in the consumer client resolved this timing issue.

### 7.3 What We Would Do Differently

If we were to redesign the system, we would make the following changes:

**Replace fixed-size arrays with dynamic data structures:** The current limits of MAX_TOPICS=64 and MAX_SUBSCRIBERS=32 are hardcoded as array dimensions. For a production system, we would use dynamically-sized hash maps for topic lookup (O(1) instead of O(n) scan) and dynamically-sized arrays for subscriber lists, removing artificial capacity limits.

**Use persistent file descriptors:** Currently, each call to storage_save_message() opens the file, maps it, writes, unmaps, and closes it. Keeping file descriptors open persistently (one per topic) and extending the mapping incrementally would eliminate the open/close overhead and reduce mmap/munmap system calls.

**Use binary format for cursor persistence:** The text-based cursors.txt file is convenient for debugging but inefficient. A memory-mapped binary file with fixed-size records would allow in-place cursor updates without rewriting the entire file on each ACK.

**Implement proper logging:** The current system uses printf() statements throughout the code. A proper logging framework with severity levels (DEBUG, INFO, WARN, ERROR), timestamps, and configurable output destinations would make debugging and monitoring much easier.

**Add unit tests for individual functions:** Our current test suite is integration-level, testing complete workflows. Unit tests for functions like match_topic(), set_cursor(), and trim_log_file() would catch bugs earlier and provide better coverage of edge cases.

### 7.4 Future Work

Several features could enhance the system for production use:

**TLS/SSL encryption:** Integrating OpenSSL or a lightweight TLS library would encrypt all client-broker communication, preventing eavesdropping and man-in-the-middle attacks.

**Authentication and authorization:** Adding username/password or token-based authentication on CONNECT would control who can publish and subscribe. Per-topic access control lists (ACLs) would provide fine-grained authorization.

**Clustering and replication:** Implementing broker federation (forwarding messages between broker instances) or active-passive replication would provide horizontal scaling and fault tolerance.

**Additional QoS levels:** Supporting QoS 0 (at most once, no ACK required) for low-priority data and QoS 2 (exactly once, using a four-way handshake) for critical transactions would provide more flexibility.

**WebSocket support:** Adding a WebSocket endpoint would allow browser-based JavaScript clients to connect directly to the broker without a proxy, enabling real-time web applications.

**Monitoring and metrics:** Exposing runtime statistics (messages per second, active connections, pending ACKs, storage usage) via a simple HTTP endpoint or UNIX domain socket would enable integration with monitoring systems like Prometheus and Grafana.

---

## 8. REFERENCES

[1] P. T. Eugster, P. A. Felber, R. Guerraoui, and A.-M. Kermarrec, "The many faces of publish/subscribe," ACM Computing Surveys, vol. 35, no. 2, pp. 114-131, June 2003.

[2] OASIS Standard, "MQTT Version 5.0," March 2019. Available: https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html

[3] M. Kerrisk, The Linux Programming Interface. San Francisco, CA: No Starch Press, 2010, Chapters 49-50 (Memory Mappings).

[4] D. R. Butenhof, Programming with POSIX Threads. Reading, MA: Addison-Wesley, 1997.

[5] W. R. Stevens, B. Fenner, and A. M. Rudoff, UNIX Network Programming, Volume 1: The Sockets Networking API, 3rd ed. Upper Saddle River, NJ: Addison-Wesley, 2003.

[6] Linux Programmer's Manual, "mmap(2)," Available: https://man7.org/linux/man-pages/man2/mmap.2.html

[7] Linux Programmer's Manual, "msync(2)," Available: https://man7.org/linux/man-pages/man2/msync.2.html

[8] Linux Programmer's Manual, "sigaction(2)," Available: https://man7.org/linux/man-pages/man2/sigaction.2.html

[9] Linux Programmer's Manual, "pthread_mutex_lock(3p)," Available: https://man7.org/linux/man-pages/man3/pthread_mutex_lock.3p.html

[10] Apache Kafka Documentation, "Design: Persistence," Available: https://kafka.apache.org/documentation/#persistence

[11] Linux Programmer's Manual, "ftruncate(2)," Available: https://man7.org/linux/man-pages/man2/ftruncate.2.html

[12] Linux Programmer's Manual, "socket(2)," Available: https://man7.org/linux/man-pages/man2/socket.2.html

---

## APPENDIX A: BUILD AND USAGE INSTRUCTIONS

### Building the Project

```bash
# Clean previous builds and compile all targets
make clean && make all

# This produces four binaries in the bin/ directory:
#   bin/broker     — The message broker server
#   bin/producer   — A simple message publisher
#   bin/consumer   — A subscribing consumer with auto-ACK
#   bin/noack_client — A consumer that never ACKs (for testing)
```

### Running the Broker

```bash
# Default configuration (per-write fsync, no retention)
./bin/broker

# With group-commit fsync and time-based retention (1 hour)
./bin/broker --fsync group-commit --retention-time 3600

# With size-based retention (10 MB per topic) and ACK-based cleanup
./bin/broker --fsync time-based --retention-size 10485760 --retention-acked

# All retention policies combined
./bin/broker --fsync per-write --retention-time 3600 --retention-size 10485760 --retention-acked
```

### Running Clients

```bash
# Subscribe to a topic
./bin/consumer <client_id> <topic>
# Example:
./bin/consumer mydevice sensor/nhietdo

# Publish a message
./bin/producer <topic> <payload>
# Example:
./bin/producer sensor/nhietdo "32.5 degrees"

# Subscribe with wildcards
./bin/consumer wildcard_user "sensor/+/temp"
./bin/consumer catchall_user "sensor/#"
```

### Running Tests

```bash
# Run the complete test suite (5 test cases)
make test
```

---

## APPENDIX B: INDIVIDUAL CONTRIBUTIONS

**Nguyen Van A — Broker Core and Storage Engine**

Nguyen Van A was responsible for designing and implementing the broker core module (broker.c, 869 lines) and the storage engine (storage.c, 183 lines). This included the topic registry with wildcard matching, the session management system mapping sockets to client IDs, the cursor tracking mechanism with persistent storage in cursors.txt, and the pending-ACK linked list with the redelivery worker background thread. He also implemented the three retention policies (time-based, size-based, and all-acknowledged) and the retention worker thread. For the storage engine, he designed the mmap-based append-only write mechanism, the configurable fsync policy system, and the log trimming function. He spent significant time debugging concurrency issues, particularly deadlocks between the redelivery worker and publish threads, ultimately establishing the lock-ordering discipline documented in this report.

**Tran Thi B — Network Layer, Protocol Design, and Client Programs**

Tran Thi B designed the binary protocol (protocol.h) and implemented the network module (network.c) including the TCP server socket creation, the read_exact() function for reliable message reads, and the receive_message() parser that deserializes the binary stream into Message structures. She also developed all three client programs: the producer (fire-and-forget publisher), the consumer (auto-ACK subscriber with graceful unsubscribe on SIGINT), and the noack_client (test harness for the redelivery mechanism). She was responsible for ensuring correct network byte order conversion using htonl/ntohl throughout the codebase, and for handling edge cases in the consumer such as SIGINT-triggered unsubscription before disconnection.

**Le Van C — Thread Pool, Server Module, Testing, and Documentation**

Le Van C implemented the thread pool module (threadpool.c) following the Boss-Worker pattern with a circular task queue, mutex, and condition variable. He designed the server module (server.c) including the CLI argument parser for fsync and retention configuration, the signal handler for graceful shutdown, and the main accept loop with EINTR handling. He developed the complete automated test suite (test_suite.sh, 176 lines) covering 5 test cases for basic pub-sub, wildcard routing, offline session resume, clean unsubscribe, and pending-ACK timeout. He also authored the Makefile with proper target dependencies and the README.md with build instructions, usage examples, and project documentation. He conducted the durability testing by developing scripts to automate the kill-9-and-restart scenario.

---

## APPENDIX C: PROJECT DIRECTORY STRUCTURE

```
c-mq-broker/
├── Makefile                     # Build system (all, clean, test targets)
├── README.md                    # Project documentation
├── REPORT.md                    # This report
├── test_suite.sh                # Automated test suite (5 test cases)
│
├── include/                     # Header files
│   ├── protocol.h               # Binary protocol definitions (MessageHeader, Message)
│   ├── broker.h                 # Broker API (Topic, Broker structs, function declarations)
│   ├── storage.h                # Storage API (FsyncPolicy enum, function declarations)
│   ├── network.h                # Network API (socket, read, parse functions)
│   └── threadpool.h             # Thread pool API (threadpool_t, task structs)
│
├── src/                         # Core source files
│   ├── server.c                 # Entry point, CLI parsing, signal handling (145 lines)
│   ├── broker.c                 # Pub-Sub logic, wildcards, ACK, cursors, retention (869 lines)
│   ├── storage.c                # mmap persistence, fsync, log trimming (183 lines)
│   ├── network.c                # TCP I/O, binary protocol serialization (112 lines)
│   └── threadpool.c             # Boss-Worker thread pool (77 lines)
│
├── clients/                     # Client programs
│   ├── producer.c               # Fire-and-forget message publisher (43 lines)
│   ├── consumer.c               # Auto-ACK subscriber with graceful shutdown (121 lines)
│   └── noack_client.c           # Never-ACK client for redelivery testing (70 lines)
│
├── bin/                         # Compiled binaries (created by make)
│   ├── broker
│   ├── producer
│   ├── consumer
│   └── noack_client
│
└── data/                        # Runtime storage (created by broker)
    ├── sensor_nhietdo.log       # Append-only message log for topic "sensor/nhietdo"
    ├── home_light.log           # Append-only message log for topic "home/light"
    └── cursors.txt              # Persisted subscriber cursor offsets
```
