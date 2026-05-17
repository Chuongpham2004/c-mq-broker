#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "../include/protocol.h"

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

    // Chuẩn bị dữ liệu
    char *topic = "sensor/nhietdo";
    char *payload = "32.5 do C";

    MessageHeader header;
    header.type = MSG_PUBLISH;
    header.topic_len = strlen(topic);
    header.payload_len = htonl(strlen(payload)); // Chuyển sang Network Byte Order

    // Gửi lần lượt Header -> Topic -> Payload
    send(sock, &header, sizeof(header), 0);
    send(sock, topic, header.topic_len, 0);
    send(sock, payload, strlen(payload), 0);

    printf("Đã gửi message thành công!\n");
    close(sock);
    return 0;
}