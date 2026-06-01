#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "../include/protocol.h"

#include <time.h>

#define PORT 8080

int main(int argc, char **argv) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Kết nối thất bại");
        return -1;
    }

    // Chuẩn bị dữ liệu (nhận từ tham số hoặc mặc định)
    char *topic = (argc > 1) ? argv[1] : "sensor/nhietdo";
    char *payload = (argc > 2) ? argv[2] : "32.5 do C";

    MessageHeader header;
    header.type = MSG_PUBLISH;
    header.topic_len = strlen(topic);
    header.msg_id = htonl(0); // Producer gửi không cần msg_id cụ thể
    header.timestamp = htonl(time(NULL));
    header.payload_len = htonl(strlen(payload)); // Chuyển sang Network Byte Order

    // Gửi lần lượt Header -> Topic -> Payload
    send(sock, &header, sizeof(header), 0);
    send(sock, topic, header.topic_len, 0);
    send(sock, payload, strlen(payload), 0);

    printf("Đã gửi message thành công tới topic '%s' với payload '%s'!\n", topic, payload);
    close(sock);
    return 0;
}