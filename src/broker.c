#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include "../include/broker.h"
#include <arpa/inet.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define DATA_DIR "./data"
#define MAX_CLIENTS 128
#define MAX_CURSORS 256

// Cấu hình Retention policies (được thiết lập từ dòng lệnh trong server.c)
int global_retention_time = 0;    // Tính theo giây, 0 = không bật
size_t global_retention_size = 0;  // Tính theo bytes, 0 = không bật
int global_retention_acked = 0;   // 1 = bật, 0 = không bật

static Broker broker;

// Cấu trúc Cursor lưu vị trí đọc của từng Client trên từng Topic
typedef struct {
    char client_id[64];
    char topic[256];
    size_t offset;
} CursorRecord;

static CursorRecord cursor_records[MAX_CURSORS];
static int cursor_count = 0;
static pthread_mutex_t cursor_lock = PTHREAD_MUTEX_INITIALIZER;

// Cấu trúc phiên kết nối đang hoạt động (socket -> client_id)
typedef struct {
    int socket;
    char client_id[64];
} ActiveSession;

static ActiveSession active_sessions[MAX_CLIENTS];
static int active_session_count = 0;
static pthread_mutex_t session_lock = PTHREAD_MUTEX_INITIALIZER;

// Biến đếm tạo ID tin nhắn duy nhất
static uint32_t global_msg_id_counter = 1;
static pthread_mutex_t msg_id_lock = PTHREAD_MUTEX_INITIALIZER;

static uint32_t get_next_global_msg_id() {
    pthread_mutex_lock(&msg_id_lock);
    uint32_t id = global_msg_id_counter++;
    if (global_msg_id_counter == 0) global_msg_id_counter = 1; // Tránh tràn số về 0
    pthread_mutex_unlock(&msg_id_lock);
    return id;
}

// Cấu trúc danh sách liên kết Pending ACK
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

static PendingAck *pending_ack_head = NULL;
static pthread_mutex_t pending_ack_lock = PTHREAD_MUTEX_INITIALIZER;

// Khai báo các hàm nội bộ về Cursor
static void load_cursors();
static void save_cursors();
static size_t get_cursor(const char *client_id, const char *topic);
static void set_cursor(const char *client_id, const char *topic, size_t offset);
static void replay_missed_messages(int client_socket, const char *client_id, const char *sub_topic);
static void cleanup_pending_acks_for_socket(int client_socket);

// Khởi tạo các giá trị ban đầu cho Broker
void init_broker() {
    broker.topic_count = 0;
    pthread_mutex_init(&broker.global_lock, NULL);
    memset(broker.topics, 0, sizeof(broker.topics));

    init_storage(); // Khởi tạo thư mục lưu trữ data/
}

// Hàm đăng ký session client
void broker_register_session(int socket, const char *client_id) {
    pthread_mutex_lock(&session_lock);
    // Gỡ kết nối cũ cùng socket hoặc client_id nếu có trùng lặp
    for (int i = 0; i < active_session_count; i++) {
        if (active_sessions[i].socket == socket || strcmp(active_sessions[i].client_id, client_id) == 0) {
            for (int j = i; j < active_session_count - 1; j++) {
                active_sessions[j] = active_sessions[j + 1];
            }
            active_session_count--;
            i--;
        }
    }
    
    if (active_session_count < MAX_CLIENTS) {
        active_sessions[active_session_count].socket = socket;
        strncpy(active_sessions[active_session_count].client_id, client_id, 63);
        active_sessions[active_session_count].client_id[63] = '\0';
        active_session_count++;
        printf("[Broker] Khởi tạo phiên kết nối: socket %d -> client_id '%s'\n", socket, client_id);
    }
    pthread_mutex_unlock(&session_lock);
}

// Hàm hủy session client
static void broker_remove_session(int socket) {
    pthread_mutex_lock(&session_lock);
    for (int i = 0; i < active_session_count; i++) {
        if (active_sessions[i].socket == socket) {
            printf("[Broker] Đã xóa phiên kết nối: socket %d (client_id '%s')\n", socket, active_sessions[i].client_id);
            for (int j = i; j < active_session_count - 1; j++) {
                active_sessions[j] = active_sessions[j + 1];
            }
            active_session_count--;
            break;
        }
    }
    pthread_mutex_unlock(&session_lock);
}

// Lấy client_id từ socket kết nối
static const char *broker_get_client_id(int socket) {
    static __thread char res[64];
    res[0] = '\0';
    pthread_mutex_lock(&session_lock);
    for (int i = 0; i < active_session_count; i++) {
        if (active_sessions[i].socket == socket) {
            strcpy(res, active_sessions[i].client_id);
            break;
        }
    }
    pthread_mutex_unlock(&session_lock);
    return res[0] != '\0' ? res : NULL;
}

// Hàm tìm hoặc tạo một Topic mới trong mảng quản lý
static Topic* find_or_create_topic(const char *topic_name) {
    pthread_mutex_lock(&broker.global_lock);

    // 1. Quét tìm xem Topic đã tồn tại chưa
    for (int i = 0; i < broker.topic_count; i++) {
        if (strcmp(broker.topics[i].name, topic_name) == 0) {
            pthread_mutex_unlock(&broker.global_lock);
            return &broker.topics[i];
        }
    }

    // 2. Nếu chưa có và chưa đầy bộ đệm, tiến hành tạo mới
    if (broker.topic_count < MAX_TOPICS) {
        int idx = broker.topic_count;
        strncpy(broker.topics[idx].name, topic_name, sizeof(broker.topics[idx].name) - 1);
        broker.topics[idx].sub_count = 0;
        pthread_mutex_init(&broker.topics[idx].topic_lock, NULL);
        broker.topic_count++;
        
        pthread_mutex_unlock(&broker.global_lock);
        printf("[Broker] Đã khởi tạo Topic mới: %s\n", topic_name);
        return &broker.topics[idx];
    }

    pthread_mutex_unlock(&broker.global_lock);
    return NULL; // Hết bộ nhớ lưu Topic
}

// Đẩy bản tin từ Producer tới toàn bộ Consumer đăng ký nhận tin (Fan-out) hỗ trợ Wildcards
void broker_publish(Message *msg) {
    if (!msg || !msg->topic) return;

    // Lưu bản tin vào đĩa cứng trước khi gửi đi, nhận lại offset kết thúc của bản tin vừa ghi
    size_t end_offset = storage_save_message(msg->topic, msg);
    if (end_offset == 0) {
        printf("[Broker] Lỗi: Không thể ghi nhận tin nhắn xuống storage\n");
        return;
    }

    // Quét qua toàn bộ topic để tìm topic đăng ký khớp (hỗ trợ wildcards)
    pthread_mutex_lock(&broker.global_lock);
    
    // Mảng lưu socket để không gửi lặp tin nhắn nếu một client subscribe nhiều pattern khớp cùng một tin
    int sent_sockets[MAX_SUBSCRIBERS * MAX_TOPICS];
    int sent_count = 0;

    for (int idx = 0; idx < broker.topic_count; idx++) {
        Topic *topic = &broker.topics[idx];
        if (match_topic(topic->name, msg->topic)) {
            pthread_mutex_lock(&topic->topic_lock);
            
            for (int i = 0; i < topic->sub_count; i++) {
                int sub_sock = topic->subscriber_sockets[i];
                
                // Kiểm tra trùng lặp
                int already_sent = 0;
                for (int s = 0; s < sent_count; s++) {
                    if (sent_sockets[s] == sub_sock) {
                        already_sent = 1;
                        break;
                    }
                }
                if (already_sent) continue;
                
                sent_sockets[sent_count++] = sub_sock;
                
                // Gắn message id mới để quản lý ACK
                uint32_t current_msg_id = get_next_global_msg_id();
                const char *client_id = broker_get_client_id(sub_sock);
                
                if (client_id != NULL) {
                    // Đưa vào danh sách Pending ACK trước khi gửi
                    pthread_mutex_lock(&pending_ack_lock);
                    PendingAck *node = malloc(sizeof(PendingAck));
                    node->client_socket = sub_sock;
                    node->msg_id = current_msg_id;
                    strcpy(node->client_id, client_id);
                    strcpy(node->topic, msg->topic); // Lưu cụ thể tên topic đã gửi
                    node->file_offset = end_offset;
                    node->payload = malloc(msg->header.payload_len);
                    memcpy(node->payload, msg->payload, msg->header.payload_len);
                    node->payload_len = msg->header.payload_len;
                    node->sent_time = time(NULL);
                    node->retry_count = 0;
                    node->next = pending_ack_head;
                    pending_ack_head = node;
                    pthread_mutex_unlock(&pending_ack_lock);
                }
                
                // Chuẩn hóa lại Header theo đúng định dạng Network Byte Order để truyền qua mạng
                MessageHeader header;
                header.type = MSG_PUBLISH;
                header.topic_len = strlen(msg->topic);
                header.msg_id = htonl(current_msg_id);
                header.timestamp = htonl(time(NULL));
                header.payload_len = htonl(msg->header.payload_len); 

                // Gửi Header cố định 14 bytes
                if (send(sub_sock, &header, sizeof(MessageHeader), 0) < 0) continue;
                // Gửi chuỗi tên Topic
                send(sub_sock, msg->topic, header.topic_len, 0);
                // Gửi khối dữ liệu Payload
                send(sub_sock, msg->payload, msg->header.payload_len, 0);
                
                printf("[Broker] Đã đẩy tin nhắn ID %u (topic: %s) tới socket mạng %d\n", current_msg_id, msg->topic, sub_sock);
            }
            
            pthread_mutex_unlock(&topic->topic_lock);
        }
    }
    
    pthread_mutex_unlock(&broker.global_lock);
}

// Ghi nhận một Consumer đăng ký nhận tin
void broker_subscribe(int client_socket, char *topic_name) {
    if (!topic_name) return;

    Topic *topic = find_or_create_topic(topic_name);
    if (!topic) return;

    pthread_mutex_lock(&topic->topic_lock);

    // Kiểm tra trùng lặp kết nối
    for (int i = 0; i < topic->sub_count; i++) {
        if (topic->subscriber_sockets[i] == client_socket) {
            pthread_mutex_unlock(&topic->topic_lock);
            return; 
        }
    }

    // Thêm socket vào danh sách lắng nghe của Topic
    if (topic->sub_count < MAX_SUBSCRIBERS) {
        topic->subscriber_sockets[topic->sub_count] = client_socket;
        topic->sub_count++;
        printf("[Broker] Subscriber socket %d đã đăng ký topic [%s] thành công\n", client_socket, topic->name);
    } else {
        printf("[Broker] Đăng ký thất bại: Topic [%s] quá tải hàng đợi Subscriber\n", topic->name);
    }

    pthread_mutex_unlock(&topic->topic_lock);
    
    // Resume-on-reconnect: Phục hồi tin nhắn cũ bị nhỡ
    const char *client_id = broker_get_client_id(client_socket);
    if (client_id != NULL) {
        replay_missed_messages(client_socket, client_id, topic_name);
    }
}

// Hủy đăng ký nhận tin từ một topic
void broker_unsubscribe(int client_socket, char *topic_name) {
    if (!topic_name) return;
    
    pthread_mutex_lock(&broker.global_lock);
    for (int i = 0; i < broker.topic_count; i++) {
        Topic *topic = &broker.topics[i];
        if (strcmp(topic->name, topic_name) == 0) {
            pthread_mutex_lock(&topic->topic_lock);
            for (int j = 0; j < topic->sub_count; j++) {
                if (topic->subscriber_sockets[j] == client_socket) {
                    for (int k = j; k < topic->sub_count - 1; k++) {
                        topic->subscriber_sockets[k] = topic->subscriber_sockets[k + 1];
                    }
                    topic->sub_count--;
                    printf("[Broker] Subscriber tại socket %d hủy đăng ký thành công topic [%s]\n", client_socket, topic->name);
                    break;
                }
            }
            pthread_mutex_unlock(&topic->topic_lock);
            break;
        }
    }
    pthread_mutex_unlock(&broker.global_lock);
}

// Xóa kết nối khỏi hệ thống khi Client tắt, tránh gửi tin vào socket rác gây lỗi hệ thống
void broker_remove_client(int client_socket) {
    pthread_mutex_lock(&broker.global_lock);
    for (int i = 0; i < broker.topic_count; i++) {
        Topic *topic = &broker.topics[i];
        pthread_mutex_lock(&topic->topic_lock);
        
        for (int j = 0; j < topic->sub_count; j++) {
            if (topic->subscriber_sockets[j] == client_socket) {
                // Dịch chuyển mảng để lấp chỗ trống
                for (int k = j; k < topic->sub_count - 1; k++) {
                    topic->subscriber_sockets[k] = topic->subscriber_sockets[k + 1];
                }
                topic->sub_count--;
                printf("[Broker] Đã gỡ subscriber socket %d khỏi topic [%s]\n", client_socket, topic->name);
                break;
            }
        }
        pthread_mutex_unlock(&topic->topic_lock);
    }
    pthread_mutex_unlock(&broker.global_lock);
    
    // Hủy session và xóa các tin nhắn Pending ACK chưa phản hồi
    broker_remove_session(client_socket);
    cleanup_pending_acks_for_socket(client_socket);
}

// Xử lý xác nhận tin nhắn thành công từ Consumer
void broker_handle_ack(int client_socket, uint32_t msg_id) {
    pthread_mutex_lock(&pending_ack_lock);
    PendingAck *prev = NULL;
    PendingAck *curr = pending_ack_head;
    while (curr != NULL) {
        if (curr->client_socket == client_socket && curr->msg_id == msg_id) {
            // Cập nhật vị trí con trỏ đọc (cursor)
            set_cursor(curr->client_id, curr->topic, curr->file_offset);
            printf("[Broker] Nhận ACK cho tin nhắn %u từ client '%s' (topic: %s, offset: %zu)\n", msg_id, curr->client_id, curr->topic, curr->file_offset);
            
            // Xóa phần tử khỏi danh sách pending ack
            PendingAck *temp = curr;
            if (prev == NULL) {
                pending_ack_head = curr->next;
            } else {
                prev->next = curr->next;
            }
            curr = curr->next;
            if (temp->payload) free(temp->payload);
            free(temp);
            pthread_mutex_unlock(&pending_ack_lock);
            return;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&pending_ack_lock);
    printf("[Broker] Cảnh báo: Nhận ACK rác không tìm thấy hoặc đã xóa (msg_id: %u) từ socket %d\n", msg_id, client_socket);
}

// Hủy bỏ các pending ACK liên quan đến socket
static void cleanup_pending_acks_for_socket(int client_socket) {
    pthread_mutex_lock(&pending_ack_lock);
    PendingAck *prev = NULL;
    PendingAck *curr = pending_ack_head;
    while (curr != NULL) {
        if (curr->client_socket == client_socket) {
            PendingAck *temp = curr;
            if (prev == NULL) {
                pending_ack_head = curr->next;
            } else {
                prev->next = curr->next;
            }
            curr = curr->next;
            if (temp->payload) free(temp->payload);
            free(temp);
        } else {
            prev = curr;
            curr = curr->next;
        }
    }
    pthread_mutex_unlock(&pending_ack_lock);
}

// Kiểm tra khớp Topic theo ký tự đại diện (Wildcards '+' và '#')
int match_topic(const char *sub, const char *pub) {
    if (sub == NULL || pub == NULL) return 0;
    const char *s = sub;
    const char *p = pub;
    
    while (1) {
        if (*s == '#') {
            // '#' khớp với tất cả các cấp độ còn lại (kể cả không có cấp độ nào)
            return 1;
        }
        
        if (*s == '+') {
            // Khớp đúng 1 cấp độ
            while (*p != '/' && *p != '\0') {
                p++;
            }
            s++; // Bỏ qua dấu '+'
            continue;
        }
        
        // Nếu ký tự trùng khớp, dịch chuyển cả hai
        if (*s == *p) {
            if (*s == '\0') {
                return 1; // Khớp hoàn toàn
            }
            s++;
            p++;
        } else {
            // Xử lý trường hợp đặc biệt: sub = "sport/#" và pub = "sport"
            if (*s == '/' && *(s+1) == '#' && *(s+2) == '\0' && *p == '\0') {
                return 1;
            }
            return 0;
        }
    }
}

// Quét thư mục lưu trữ và replay các tin nhắn chưa đọc đối với subscriber cụ thể (hỗ trợ wildcard)
static void replay_missed_messages(int client_socket, const char *client_id, const char *sub_topic) {
    DIR *dir = opendir(DATA_DIR);
    if (!dir) return;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        size_t len = strlen(entry->d_name);
        if (len > 4 && strcmp(entry->d_name + len - 4, ".log") == 0) {
            char filename[512];
            strncpy(filename, entry->d_name, len - 4);
            filename[len - 4] = '\0';
            
            // Lấy tên topic gốc bằng cách đọc gói tin đầu tiên trong file log
            char orig_topic[256] = "";
            char filepath[512];
            snprintf(filepath, sizeof(filepath), "%s/%s", DATA_DIR, entry->d_name);
            
            pthread_mutex_lock(&storage_mutex);
            FILE *f = fopen(filepath, "rb");
            if (f) {
                MessageHeader h;
                if (fread(&h, 1, sizeof(MessageHeader), f) == sizeof(MessageHeader)) {
                    if (h.topic_len > 0) {
                        if (fread(orig_topic, 1, h.topic_len, f) != h.topic_len) {
                            orig_topic[0] = '\0';
                        } else {
                            orig_topic[h.topic_len] = '\0';
                        }
                    }
                }
                fclose(f);
            }
            
            // Fallback nếu không đọc được từ file log
            if (strlen(orig_topic) == 0) {
                for (int i = 0; filename[i]; i++) {
                    if (filename[i] == '_') filename[i] = '/';
                }
                strcpy(orig_topic, filename);
            }
            
            // So sánh xem topic gốc có khớp với topic đăng ký không
            if (match_topic(sub_topic, orig_topic)) {
                size_t cursor = get_cursor(client_id, orig_topic);
                
                int fd = open(filepath, O_RDONLY);
                if (fd != -1) {
                    struct stat st;
                    if (fstat(fd, &st) == 0 && (size_t)st.st_size > cursor) {
                        size_t file_size = st.st_size;
                        void *map = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
                        if (map != MAP_FAILED) {
                            size_t offset = cursor;
                            while (offset + sizeof(MessageHeader) <= file_size) {
                                MessageHeader *h = (MessageHeader *)((char *)map + offset);
                                uint32_t payload_len = ntohl(h->payload_len);
                                size_t record_size = sizeof(MessageHeader) + h->topic_len + payload_len;
                                
                                if (offset + record_size > file_size) {
                                    break; // Bản tin bị lỗi/không hoàn chỉnh
                                }
                                
                                // Gửi bản tin cũ này tới client và lưu vào Pending ACK list
                                uint32_t current_msg_id = get_next_global_msg_id();
                                
                                pthread_mutex_lock(&pending_ack_lock);
                                PendingAck *node = malloc(sizeof(PendingAck));
                                node->client_socket = client_socket;
                                node->msg_id = current_msg_id;
                                strcpy(node->client_id, client_id);
                                strcpy(node->topic, orig_topic);
                                node->file_offset = offset + record_size;
                                node->payload = malloc(payload_len);
                                memcpy(node->payload, (char *)map + offset + sizeof(MessageHeader) + h->topic_len, payload_len);
                                node->payload_len = payload_len;
                                node->sent_time = time(NULL);
                                node->retry_count = 0;
                                node->next = pending_ack_head;
                                pending_ack_head = node;
                                pthread_mutex_unlock(&pending_ack_lock);
                                
                                MessageHeader header;
                                header.type = MSG_PUBLISH;
                                header.topic_len = strlen(orig_topic);
                                header.msg_id = htonl(current_msg_id);
                                header.timestamp = h->timestamp; // Giữ nguyên nhãn thời gian lưu trữ gốc
                                header.payload_len = htonl(payload_len);
                                
                                send(client_socket, &header, sizeof(MessageHeader), 0);
                                send(client_socket, orig_topic, header.topic_len, 0);
                                send(client_socket, node->payload, payload_len, 0);
                                
                                printf("[Broker] Replay tin nhắn ID %u (topic: %s) từ file log ở offset %zu tới client '%s'\n", current_msg_id, orig_topic, offset, client_id);
                                
                                offset += record_size;
                            }
                            munmap(map, file_size);
                        }
                        close(fd);
                    } else {
                        close(fd);
                    }
                }
            }
            pthread_mutex_unlock(&storage_mutex);
        }
    }
    closedir(dir);
}

// Tiến hành lưu trữ các cursor đọc của Consumer vào đĩa cứng
static void load_cursors() {
    pthread_mutex_lock(&cursor_lock);
    cursor_count = 0;
    FILE *f = fopen("./data/cursors.txt", "r");
    if (!f) {
        pthread_mutex_unlock(&cursor_lock);
        return;
    }
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        if (cursor_count >= MAX_CURSORS) break;
        CursorRecord rec;
        if (sscanf(line, "%63[^:]:%255[^:]:%zu", rec.client_id, rec.topic, &rec.offset) == 3) {
            cursor_records[cursor_count++] = rec;
        }
    }
    fclose(f);
    pthread_mutex_unlock(&cursor_lock);
    printf("[Broker] Đã load %d cursor(s) từ cursors.txt\n", cursor_count);
}

static void save_cursors() {
    pthread_mutex_lock(&cursor_lock);
    FILE *f = fopen("./data/cursors.txt", "w");
    if (f) {
        for (int i = 0; i < cursor_count; i++) {
            fprintf(f, "%s:%s:%zu\n", cursor_records[i].client_id, cursor_records[i].topic, cursor_records[i].offset);
        }
        fclose(f);
    }
    pthread_mutex_unlock(&cursor_lock);
}

static size_t get_cursor(const char *client_id, const char *topic) {
    if (!client_id || !topic) return 0;
    size_t offset = 0;
    pthread_mutex_lock(&cursor_lock);
    for (int i = 0; i < cursor_count; i++) {
        if (strcmp(cursor_records[i].client_id, client_id) == 0 && strcmp(cursor_records[i].topic, topic) == 0) {
            offset = cursor_records[i].offset;
            break;
        }
    }
    pthread_mutex_unlock(&cursor_lock);
    return offset;
}

static void set_cursor(const char *client_id, const char *topic, size_t offset) {
    if (!client_id || !topic) return;
    pthread_mutex_lock(&cursor_lock);
    int found = 0;
    for (int i = 0; i < cursor_count; i++) {
        if (strcmp(cursor_records[i].client_id, client_id) == 0 && strcmp(cursor_records[i].topic, topic) == 0) {
            if (offset > cursor_records[i].offset) {
                cursor_records[i].offset = offset;
            }
            found = 1;
            break;
        }
    }
    if (!found && cursor_count < MAX_CURSORS) {
        strncpy(cursor_records[cursor_count].client_id, client_id, 63);
        cursor_records[cursor_count].client_id[63] = '\0';
        strncpy(cursor_records[cursor_count].topic, topic, 255);
        cursor_records[cursor_count].topic[255] = '\0';
        cursor_records[cursor_count].offset = offset;
        cursor_count++;
    }
    pthread_mutex_unlock(&cursor_lock);
    save_cursors();
}

// Luồng kiểm tra và gửi lại các tin nhắn Pending ACK (Redelivery)
static void *redelivery_worker(void *arg) {
    (void)arg;
    while (1) {
        sleep(1);
        pthread_mutex_lock(&pending_ack_lock);
        PendingAck *prev = NULL;
        PendingAck *curr = pending_ack_head;
        time_t now = time(NULL);
        while (curr != NULL) {
            if (now - curr->sent_time >= 5) {
                curr->retry_count++;
                if (curr->retry_count > 3) {
                    printf("[Broker] Client '%s' (socket %d) không ACK tin nhắn %u sau 3 lần gửi lại. Đang ngắt kết nối...\n", curr->client_id, curr->client_socket, curr->msg_id);
                    shutdown(curr->client_socket, SHUT_RDWR);
                    close(curr->client_socket);
                    
                    // Gỡ nút này khỏi danh sách liên kết
                    PendingAck *temp = curr;
                    if (prev == NULL) {
                        pending_ack_head = curr->next;
                    } else {
                        prev->next = curr->next;
                    }
                    curr = curr->next;
                    if (temp->payload) free(temp->payload);
                    free(temp);
                    continue; // Bỏ qua phần gán dịch chuyển thông thường bên dưới
                } else {
                    printf("[Broker] Gửi lại tin nhắn ID %u (lần %d) tới socket %d...\n", curr->msg_id, curr->retry_count, curr->client_socket);
                    
                    MessageHeader header;
                    header.type = MSG_PUBLISH;
                    header.topic_len = strlen(curr->topic);
                    header.msg_id = htonl(curr->msg_id);
                    header.timestamp = htonl(now);
                    header.payload_len = htonl(curr->payload_len);
                    
                    send(curr->client_socket, &header, sizeof(MessageHeader), 0);
                    send(curr->client_socket, curr->topic, header.topic_len, 0);
                    send(curr->client_socket, curr->payload, curr->payload_len, 0);
                    
                    curr->sent_time = now;
                }
            }
            prev = curr;
            curr = curr->next;
        }
        pthread_mutex_unlock(&pending_ack_lock);
    }
    return NULL;
}

// Xử lý các nghiệp vụ Retention cleanup
static void process_retention_for_file(const char *filename) {
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", DATA_DIR, filename);
    
    int fd = open(filepath, O_RDONLY);
    if (fd == -1) return;
    
    struct stat st;
    if (fstat(fd, &st) == -1 || st.st_size == 0) {
        close(fd);
        return;
    }
    size_t file_size = st.st_size;
    
    void *map = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        close(fd);
        return;
    }
    
    char orig_topic[256] = "";
    MessageHeader *first_h = (MessageHeader *)map;
    if (file_size >= sizeof(MessageHeader) && first_h->topic_len > 0) {
        if (file_size >= sizeof(MessageHeader) + first_h->topic_len) {
            memcpy(orig_topic, (char *)map + sizeof(MessageHeader), first_h->topic_len);
            orig_topic[first_h->topic_len] = '\0';
        }
    }
    
    if (strlen(orig_topic) == 0) {
        char temp_name[512];
        size_t len = strlen(filename);
        strncpy(temp_name, filename, len - 4);
        temp_name[len - 4] = '\0';
        for (int i = 0; temp_name[i]; i++) {
            if (temp_name[i] == '_') temp_name[i] = '/';
        }
        strcpy(orig_topic, temp_name);
    }
    
    size_t time_bytes_to_remove = 0;
    size_t size_bytes_to_remove = 0;
    size_t acked_bytes_to_remove = 0;
    
    // 1. Retention theo thời gian
    if (global_retention_time > 0) {
        time_t cutoff = time(NULL) - global_retention_time;
        size_t offset = 0;
        while (offset + sizeof(MessageHeader) <= file_size) {
            MessageHeader *h = (MessageHeader *)((char *)map + offset);
            uint32_t payload_len = ntohl(h->payload_len);
            uint32_t msg_ts = ntohl(h->timestamp);
            size_t record_size = sizeof(MessageHeader) + h->topic_len + payload_len;
            if (offset + record_size > file_size) break;
            
            if (msg_ts < (uint32_t)cutoff) {
                time_bytes_to_remove += record_size;
            } else {
                break;
            }
            offset += record_size;
        }
    }
    
    // 2. Retention theo dung lượng tối đa
    if (global_retention_size > 0 && file_size > global_retention_size) {
        size_t offset = 0;
        while (offset + sizeof(MessageHeader) <= file_size) {
            MessageHeader *h = (MessageHeader *)((char *)map + offset);
            uint32_t payload_len = ntohl(h->payload_len);
            size_t record_size = sizeof(MessageHeader) + h->topic_len + payload_len;
            if (offset + record_size > file_size) break;
            
            if (file_size - offset > global_retention_size) {
                size_bytes_to_remove += record_size;
            } else {
                break;
            }
            offset += record_size;
        }
    }
    
    // 3. Retention khi đã được tất cả subscriber ACK
    if (global_retention_acked) {
        size_t min_offset = (size_t)-1;
        int has_subscribers = 0;
        pthread_mutex_lock(&cursor_lock);
        for (int i = 0; i < cursor_count; i++) {
            if (strcmp(cursor_records[i].topic, orig_topic) == 0) {
                has_subscribers = 1;
                if (cursor_records[i].offset < min_offset) {
                    min_offset = cursor_records[i].offset;
                }
            }
        }
        pthread_mutex_unlock(&cursor_lock);
        
        if (has_subscribers && min_offset != (size_t)-1) {
            size_t offset = 0;
            while (offset + sizeof(MessageHeader) <= file_size) {
                MessageHeader *h = (MessageHeader *)((char *)map + offset);
                uint32_t payload_len = ntohl(h->payload_len);
                size_t record_size = sizeof(MessageHeader) + h->topic_len + payload_len;
                if (offset + record_size > file_size) break;
                
                if (offset + record_size <= min_offset) {
                    acked_bytes_to_remove += record_size;
                } else {
                    break;
                }
                offset += record_size;
            }
        }
    }
    
    munmap(map, file_size);
    close(fd);
    
    size_t bytes_to_remove = time_bytes_to_remove;
    if (size_bytes_to_remove > bytes_to_remove) bytes_to_remove = size_bytes_to_remove;
    if (acked_bytes_to_remove > bytes_to_remove) bytes_to_remove = acked_bytes_to_remove;
    
    if (bytes_to_remove > 0) {
        trim_log_file(filepath, bytes_to_remove);
        
        // Điều chỉnh lại cursor offset tương ứng trong RAM
        pthread_mutex_lock(&cursor_lock);
        for (int i = 0; i < cursor_count; i++) {
            if (strcmp(cursor_records[i].topic, orig_topic) == 0) {
                if (cursor_records[i].offset < bytes_to_remove) {
                    cursor_records[i].offset = 0;
                } else {
                    cursor_records[i].offset -= bytes_to_remove;
                }
            }
        }
        pthread_mutex_unlock(&cursor_lock);
        save_cursors();
        
        // Cập nhật offset các bản tin pending ACK
        pthread_mutex_lock(&pending_ack_lock);
        PendingAck *curr = pending_ack_head;
        while (curr != NULL) {
            if (strcmp(curr->topic, orig_topic) == 0) {
                if (curr->file_offset < bytes_to_remove) {
                    curr->file_offset = 0;
                } else {
                    curr->file_offset -= bytes_to_remove;
                }
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&pending_ack_lock);
        printf("[Broker] Retention cleanup hoàn tất cho topic [%s], đã giải phóng %zu bytes\n", orig_topic, bytes_to_remove);
    }
}

// Luồng chạy định kỳ để dọn dẹp các bản tin theo các chính sách Retention
static void *retention_worker(void *arg) {
    (void)arg;
    while (1) {
        sleep(5); // Chạy dọn dẹp sau mỗi 5 giây cho môi trường thử nghiệm
        
        DIR *dir = opendir(DATA_DIR);
        if (!dir) continue;
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            size_t len = strlen(entry->d_name);
            if (len > 4 && strcmp(entry->d_name + len - 4, ".log") == 0) {
                process_retention_for_file(entry->d_name);
            }
        }
        closedir(dir);
    }
    return NULL;
}

// Hàm khởi tạo các luồng chạy ngầm của Broker
void init_broker_threads() {
    pthread_t redelivery_tid;
    pthread_t retention_tid;
    
    pthread_create(&redelivery_tid, NULL, redelivery_worker, NULL);
    pthread_detach(redelivery_tid);
    
    pthread_create(&retention_tid, NULL, retention_worker, NULL);
    pthread_detach(retention_tid);
    
    load_cursors(); // Đọc cursor từ disk
}