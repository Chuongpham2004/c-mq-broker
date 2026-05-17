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

#endif // BROKER_H