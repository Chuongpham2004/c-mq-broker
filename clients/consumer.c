#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
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
        perror("Kết nối tới Broker thất bại");
        return -1;
    }

    // 1. Gửi bản tin Subscribe tới một Topic cụ thể
    char *topic = "sensor/nhietdo";
    MessageHeader header;
    header.type = MSG_SUBSCRIBE;
    header.topic_len = strlen(topic);
    header.payload_len = htonl(0); // Đăng ký nhận tin thì không có nội dung payload gửi đi

    send(sock, &header, sizeof(header), 0);
    send(sock, topic, header.topic_len, 0);
    printf("Đã gửi yêu cầu Subscribe topic [%s]. Đang chờ nhận tin nhắn...\n\n", topic);

    // 2. Treo luồng để liên tục lắng nghe tin nhắn đổ về từ Broker
    while (1) {
        // Tận dụng chính bộ parser receive_message của hệ thống để dịch ngược nhị phân dữ liệu nhận về
        Message *msg = receive_message(sock);
        if (!msg) {
            printf("Mất kết nối tới Broker trung tâm.\n");
            break;
        }

        if (msg->header.type == MSG_PUBLISH) {
            printf("[NHẬN TIN] Topic: %s -> Nội dung: %s\n", msg->topic, (char*)msg->payload);
        }
        free(msg);
    }

    close(sock);
    return 0;
}