#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include "../include/protocol.h"
#include "../include/network.h"

#define PORT 8080

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Kết nối thất bại");
        return -1;
    }

    // 1. Gửi kết nối chứa Client ID
    char *client_id = "noack_consumer";
    MessageHeader conn_hdr;
    conn_hdr.type = MSG_CONNECT;
    conn_hdr.topic_len = 0;
    conn_hdr.msg_id = htonl(0);
    conn_hdr.timestamp = htonl(time(NULL));
    conn_hdr.payload_len = htonl(strlen(client_id));
    send(sock, &conn_hdr, sizeof(conn_hdr), 0);
    send(sock, client_id, strlen(client_id), 0);

    // 2. Gửi đăng ký
    char *topic = "sensor/noack";
    MessageHeader sub_hdr;
    sub_hdr.type = MSG_SUBSCRIBE;
    sub_hdr.topic_len = strlen(topic);
    sub_hdr.msg_id = htonl(0);
    sub_hdr.timestamp = htonl(time(NULL));
    sub_hdr.payload_len = htonl(0);
    send(sock, &sub_hdr, sizeof(sub_hdr), 0);
    send(sock, topic, sub_hdr.topic_len, 0);
    
    printf("[NoACK] Đã đăng ký topic '%s', đang đợi tin nhắn...\n", topic);

    // 3. Đọc tin nhắn nhưng KHÔNG phản hồi ACK
    Message *msg = receive_message(sock);
    if (msg) {
        printf("[NoACK] Đã nhận tin nhắn: '%s'. Nhưng KHÔNG gửi ACK phản hồi!\n", (char *)msg->payload);
        free_message(msg);
    }

    // Đọc liên tục cho đến khi Broker ngắt kết nối hẳn (Timeout)
    printf("[NoACK] Đang chờ Broker ngắt kết nối do quá hạn ACK...\n");
    while (1) {
        Message *msg2 = receive_message(sock);
        if (!msg2) {
            printf("[NoACK] Broker đã chủ động ngắt kết nối do quá hạn ACK (Timeout)!\n");
            break;
        }
        printf("[NoACK] Nhận lại tin nhắn gửi lại (ID: %u). Vẫn tiếp tục lờ đi...\n", msg2->header.msg_id);
        free_message(msg2);
    }

    close(sock);
    return 0;
}
