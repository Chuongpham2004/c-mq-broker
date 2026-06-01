#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include "../include/network.h"

// Hàm khởi tạo TCP Server Socket
int create_server_socket(int port) {
    int server_fd;
    struct sockaddr_in server_addr;
    int opt = 1;

    // 1. Tạo file descriptor cho socket (IPv4, TCP)
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Thiết lập tùy chọn SO_REUSEADDR để tránh lỗi "Address already in use"
    // khi bạn tắt và bật lại server liên tục trong lúc test.
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("setsockopt failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 2. Định nghĩa địa chỉ và port cho server
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY; // Lắng nghe trên mọi interface
    server_addr.sin_port = htons(port);       // Chuyển port sang network byte order

    // 3. Bind socket với port
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 4. Bắt đầu lắng nghe kết nối (Backlog = 128 connections chờ)
    if (listen(server_fd, 128) == -1) {
        perror("Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Broker started. Listening on port %d...\n", port);
    return server_fd;
}

ssize_t read_exact(int fd, void *buf, size_t n) {
    size_t nleft = n;
    ssize_t nread;
    char *ptr = buf;
    while (nleft > 0) {
        if ((nread = read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR) return -1; // Bị ngắt bởi tín hiệu (Graceful shutdown)
            else return -1; // Lỗi thật sự
        } else if (nread == 0) break; // EOF (Client ngắt kết nối)
        nleft -= nread;
        ptr += nread;
    }
    return n - nleft; 
}

// Phân tích byte stream thành struct Message
Message* receive_message(int client_socket) {
    Message *msg = malloc(sizeof(Message));
    if (!msg) return NULL;
    memset(msg, 0, sizeof(Message));

    // 1. Đọc đúng Header (14 bytes)
    if (read_exact(client_socket, &msg->header, sizeof(MessageHeader)) != sizeof(MessageHeader)) {
        free(msg);
        return NULL; 
    }

    // Đảo ngược byte order từ Network (Big Endian) sang Host (Little Endian)
    msg->header.msg_id = ntohl(msg->header.msg_id);
    msg->header.timestamp = ntohl(msg->header.timestamp);
    msg->header.payload_len = ntohl(msg->header.payload_len);

    // 2. Đọc Topic (nếu có)
    if (msg->header.topic_len > 0) {
        msg->topic = calloc(msg->header.topic_len + 1, 1); 
        if (read_exact(client_socket, msg->topic, msg->header.topic_len) != msg->header.topic_len) {
            free(msg->topic); free(msg); return NULL;
        }
    }

    // 3. Đọc Payload (nếu có)
    if (msg->header.payload_len > 0) {
        msg->payload = malloc(msg->header.payload_len + 1); 
        memset(msg->payload, 0, msg->header.payload_len + 1);
        if (read_exact(client_socket, msg->payload, msg->header.payload_len) != msg->header.payload_len) {
            if (msg->topic) free(msg->topic);
            free(msg->payload); free(msg); return NULL;
        }
    }
    return msg;
}

// Dọn dẹp bộ nhớ chống memory leak
void free_message(Message *msg) {
    if (!msg) return;
    if (msg->topic) free(msg->topic);
    if (msg->payload) free(msg->payload);
    free(msg);
}