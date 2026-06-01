#ifndef BROKER_H
#define BROKER_H

#include <pthread.h>
#include "protocol.h"
#include "../include/storage.h"

#define MAX_TOPICS 64
#define MAX_SUBSCRIBERS 32

// Cấu trúc quản lý một Topic cụ thể
typedef struct {
    char name[256];                         // Tên Topic (Ví dụ: "sensor/nhietdo")
    int subscriber_sockets[MAX_SUBSCRIBERS];// Danh sách socket của các Consumer đang hóng tin
    int sub_count;                          // Số lượng Consumer hiện tại
    pthread_mutex_t topic_lock;             // Khóa Mutex riêng cho từng Topic
} Topic;

// Cấu trúc tổng điều phối của Message Broker
typedef struct {
    Topic topics[MAX_TOPICS];
    int topic_count;
    pthread_mutex_t global_lock;            // Khóa tổng khi thêm/sửa danh sách Topic
} Broker;

// Các hàm xử lý nghiệp vụ Pub/Sub
void init_broker();
void broker_publish(Message *msg);
void broker_subscribe(int client_socket, char *topic_name);
void broker_remove_client(int client_socket);

void broker_unsubscribe(int client_socket, char *topic_name);
int match_topic(const char *sub, const char *pub);
void broker_register_session(int socket, const char *client_id);
void broker_handle_ack(int client_socket, uint32_t msg_id);
void init_broker_threads();

extern int global_retention_time;
extern size_t global_retention_size;
extern int global_retention_acked;

#endif // BROKER_H