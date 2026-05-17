#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>

// Định nghĩa các loại request (Command Type)
typedef enum {
    MSG_CONNECT = 1,  // Client báo kết nối
    MSG_PUBLISH = 2,  // Client gửi tin nhắn vào Topic
    MSG_SUBSCRIBE = 3,// Client đăng ký nhận tin từ Topic
    MSG_ACK = 4       // Server phản hồi thành công
} MessageType;

// Cấu trúc Header cố định (Tổng cộng 6 bytes)
// Sử dụng __attribute__((packed)) để tránh compiler tự động padding bộ nhớ,
// đảm bảo gửi qua mạng đúng từng byte.
typedef struct __attribute__((packed)) {
    uint8_t  type;          // Loại tin nhắn (1 byte)
    uint8_t  topic_len;     // Độ dài tên Topic (1 byte, tối đa 255 ký tự)
    uint32_t payload_len;   // Độ dài nội dung tin nhắn (4 bytes)
} MessageHeader;

// Cấu trúc chứa toàn bộ một gói tin sau khi phân tích
typedef struct {
    MessageHeader header;
    char *topic;    // Chuỗi chứa tên topic (cấp phát động)
    void *payload;  // Dữ liệu nội dung (cấp phát động)
} Message;

#endif // PROTOCOL_H