#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include "../include/broker.h"
#include <arpa/inet.h>

static Broker broker;

// Khởi tạo các giá trị ban đầu cho Broker
void init_broker() {
    broker.topic_count = 0;
    pthread_mutex_init(&broker.global_lock, NULL);
    memset(broker.topics, 0, sizeof(broker.topics));

    init_storage(); // Khởi tạo thư mục lưu trữ data/
}

// Hàm nội bộ: Tìm hoặc tạo một Topic mới trong mảng quản lý
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

// Đẩy bản tin từ Producer tới toàn bộ Consumer đăng ký nhận tin (Fan-out)
void broker_publish(Message *msg) {
    if (!msg || !msg->topic) return;

    storage_save_message(msg->topic, msg); // Lưu bản tin vào đĩa cứng trước khi gửi đi
    Topic *topic = find_or_create_topic(msg->topic);
    if (!topic) {
        printf("[Broker] Lỗi: Không thể xử lý Topic %s (Đầy bộ nhớ hệ thống)\n", msg->topic);
        return;
    }

    // Khóa cục bộ Topic này lại để bảo vệ mảng subscriber_sockets khỏi Race Condition
    pthread_mutex_lock(&topic->topic_lock);

    printf("[Broker] Nhận tin nhắn mới từ topic [%s]. Đang chuyển tiếp tới %d subscribers...\n", 
           topic->name, topic->sub_count);

    // Chuẩn hóa lại Header theo đúng định dạng Network Byte Order để truyền qua mạng
    MessageHeader header;
    header.type = MSG_PUBLISH;
    header.topic_len = strlen(topic->name);
    header.payload_len = htonl(msg->header.payload_len); 

    // Vòng lặp truyền tin song song đến các socket
    for (int i = 0; i < topic->sub_count; i++) {
        int sub_sock = topic->subscriber_sockets[i];
        
        // Gửi Header cố định 6 bytes
        if (send(sub_sock, &header, sizeof(MessageHeader), 0) < 0) continue;
        // Gửi chuỗi tên Topic
        send(sub_sock, topic->name, header.topic_len, 0);
        // Gửi khối dữ liệu Payload
        send(sub_sock, msg->payload, msg->header.payload_len, 0);
        
        printf("  -> Đã đẩy tin qua dòng mạng tới socket mạng %d\n", sub_sock);
    }

    pthread_mutex_unlock(&topic->topic_lock);
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
        printf("[Broker] Subscriber tại socket %d đã đăng ký topic [%s] thành công\n", client_socket, topic->name);
    } else {
        printf("[Broker] Đăng ký thất bại: Topic [%s] quá tải hàng đợi Subscriber\n", topic->name);
    }

    pthread_mutex_unlock(&topic->topic_lock);
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
                printf("[Broker] Hủy kích hoạt luồng: Đã gỡ socket %d khỏi topic [%s]\n", client_socket, topic->name);
                break;
            }
        }
        pthread_mutex_unlock(&topic->topic_lock);
    }
    pthread_mutex_unlock(&broker.global_lock);
}