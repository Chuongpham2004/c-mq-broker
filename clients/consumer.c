#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "../include/protocol.h"
#include "../include/network.h"

#define PORT 8080

#include <signal.h>
#include <time.h>

#define PORT 8080

int keep_running = 1;
int sock = -1;
char *client_id = "consumer_default";
char *topic = "sensor/nhietdo";

void handle_sigint(int sig) {
    (void)sig;
    printf("\n[Consumer] Bắt đầu tắt máy an toàn...\n");
    keep_running = 0;
    
    // Gửi yêu cầu UNSUBSCRIBE trước khi đóng socket
    if (sock != -1) {
        MessageHeader unsub_hdr;
        unsub_hdr.type = MSG_UNSUBSCRIBE;
        unsub_hdr.topic_len = strlen(topic);
        unsub_hdr.msg_id = htonl(0);
        unsub_hdr.timestamp = htonl(time(NULL));
        unsub_hdr.payload_len = htonl(0);
        
        send(sock, &unsub_hdr, sizeof(unsub_hdr), 0);
        send(sock, topic, unsub_hdr.topic_len, 0);
        printf("[Consumer] Đã gửi yêu cầu UNSUBSCRIBE cho topic [%s]\n", topic);
    }
}

int main(int argc, char **argv) {
    // Đăng ký tín hiệu tắt máy
    signal(SIGINT, handle_sigint);

    if (argc > 1) {
        client_id = argv[1];
    }
    if (argc > 2) {
        topic = argv[2];
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Kết nối tới Broker thất bại");
        return -1;
    }

    printf("[Consumer] Kết nối thành công. Client ID: '%s', Topic: '%s'\n", client_id, topic);

    // 1. Gửi bản tin Connect chứa Client ID trong payload
    MessageHeader conn_hdr;
    conn_hdr.type = MSG_CONNECT;
    conn_hdr.topic_len = 0;
    conn_hdr.msg_id = htonl(0);
    conn_hdr.timestamp = htonl(time(NULL));
    conn_hdr.payload_len = htonl(strlen(client_id));
    
    send(sock, &conn_hdr, sizeof(conn_hdr), 0);
    send(sock, client_id, strlen(client_id), 0);

    // 2. Gửi bản tin Subscribe tới một Topic cụ thể
    MessageHeader sub_hdr;
    sub_hdr.type = MSG_SUBSCRIBE;
    sub_hdr.topic_len = strlen(topic);
    sub_hdr.msg_id = htonl(0);
    sub_hdr.timestamp = htonl(time(NULL));
    sub_hdr.payload_len = htonl(0); // Đăng ký nhận tin thì không có nội dung payload gửi đi

    send(sock, &sub_hdr, sizeof(sub_hdr), 0);
    send(sock, topic, sub_hdr.topic_len, 0);
    printf("[Consumer] Đã gửi yêu cầu SUBSCRIBE topic [%s]. Đang chờ nhận tin nhắn...\n\n", topic);

    // 3. Treo luồng để liên tục lắng nghe tin nhắn đổ về từ Broker
    while (keep_running) {
        // Tận dụng chính bộ parser receive_message của hệ thống để dịch ngược nhị phân dữ liệu nhận về
        Message *msg = receive_message(sock);
        if (!msg) {
            if (keep_running) {
                printf("[Consumer] Mất kết nối tới Broker trung tâm.\n");
            }
            break;
        }

        if (msg->header.type == MSG_PUBLISH) {
            printf("[NHẬN TIN] ID: %u | Topic: %s -> Nội dung: %s\n", msg->header.msg_id, msg->topic, (char*)msg->payload);
            
            // Tự động gửi ACK phản hồi
            MessageHeader ack_hdr;
            ack_hdr.type = MSG_ACK;
            ack_hdr.topic_len = msg->header.topic_len;
            ack_hdr.msg_id = htonl(msg->header.msg_id);
            ack_hdr.timestamp = htonl(time(NULL));
            ack_hdr.payload_len = htonl(0);
            
            send(sock, &ack_hdr, sizeof(ack_hdr), 0);
            if (msg->header.topic_len > 0) {
                send(sock, msg->topic, msg->header.topic_len, 0);
            }
            printf("  -> Đã gửi ACK cho tin nhắn ID %u\n", msg->header.msg_id);
        }
        free_message(msg);
    }

    close(sock);
    return 0;
}