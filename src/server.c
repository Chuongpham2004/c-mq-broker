#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "../include/network.h"
#include "../include/threadpool.h"
#include "../include/protocol.h"
#include "../include/broker.h"
#include <signal.h>
#include <errno.h>

#define PORT 8080
#define THREAD_COUNT 4    // Giả lập 4 Worker threads
#define QUEUE_SIZE 100    // Hàng đợi chứa tối đa 100 kết nối

// Sử dụng sig_atomic_t để đảm bảo an toàn bộ nhớ khi thao tác giữa các luồng tín hiệu
volatile sig_atomic_t keep_running = 1;

// Hàm này sẽ tự động được gọi khi bạn nhấn Ctrl + C
void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        printf("\n\n[System] Nhận tín hiệu ngắt (Signal %d). Bắt đầu quá trình Graceful Shutdown...\n", sig);
        keep_running = 0; // Đổi cờ để thoát vòng lặp vô tận
    }
}

// Hàm xử lý client (Sẽ do Worker thread thực thi)
void handle_client(void *arg) {
    int client_socket = *(int *)arg;
    free(arg); 

    printf("[Worker %lu] Đang xử lý client socket %d...\n", pthread_self(), client_socket);
    
    // Liên tục đọc gói tin từ client cho đến khi họ ngắt kết nối
    while (1) {
        Message *msg = receive_message(client_socket);
        if (msg == NULL) {
            printf("[Worker %lu] Client %d ngắt kết nối.\n", pthread_self(), client_socket);
            break;
        }

        if (msg->header.type == MSG_CONNECT) {
            if (msg->payload) {
                broker_register_session(client_socket, (char *)msg->payload);
            }
        } else if (msg->header.type == MSG_PUBLISH) {
            broker_publish(msg);
        } else if (msg->header.type == MSG_SUBSCRIBE) {
            broker_subscribe(client_socket, msg->topic);
        } else if (msg->header.type == MSG_UNSUBSCRIBE) {
            broker_unsubscribe(client_socket, msg->topic);
        } else if (msg->header.type == MSG_ACK) {
            broker_handle_ack(client_socket, msg->header.msg_id);
        }

        free_message(msg);
    }
    // Gỡ client khỏi các topics khi ngắt kết nối
    broker_remove_client(client_socket);
    close(client_socket);
}

int main(int argc, char **argv) {
    init_broker(); 
    
    // Phân tích các tham số dòng lệnh cấu hình fsync & retention
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--fsync") == 0 && i + 1 < argc) {
            char *policy = argv[++i];
            if (strcmp(policy, "per-write") == 0) {
                global_fsync_policy = FSYNC_PER_WRITE;
            } else if (strcmp(policy, "group-commit") == 0) {
                global_fsync_policy = FSYNC_GROUP_COMMIT;
            } else if (strcmp(policy, "time-based") == 0) {
                global_fsync_policy = FSYNC_TIME_BASED;
            }
        } else if (strcmp(argv[i], "--retention-time") == 0 && i + 1 < argc) {
            global_retention_time = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--retention-size") == 0 && i + 1 < argc) {
            global_retention_size = strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--retention-acked") == 0) {
            global_retention_acked = 1;
        }
    }
    
    // Khởi động các luồng phụ trách gửi lại và dọn dẹp tin nhắn
    init_broker_threads();

    // Đăng ký bắt tín hiệu hệ thống (SIGINT = Ctrl+C, SIGTERM = lệnh kill)
    struct sigaction sa;
    sa.sa_handler = handle_signal;
    sa.sa_flags = 0; // KHÔNG dùng SA_RESTART để đảm bảo hàm accept() bị ngắt ngay lập tức
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    int server_socket = create_server_socket(PORT);
    threadpool_t *pool = threadpool_create(THREAD_COUNT, QUEUE_SIZE);
    printf("Hệ thống Message Broker đã sẵn sàng điều phối...\n");
    printf("Chính sách Fsync: %s\n", 
           global_fsync_policy == FSYNC_PER_WRITE ? "per-write" :
           global_fsync_policy == FSYNC_GROUP_COMMIT ? "group-commit" : "time-based");
    if (global_retention_time > 0) printf("Chính sách Retention Time: %d giây\n", global_retention_time);
    if (global_retention_size > 0) printf("Chính sách Retention Size: %zu bytes\n", global_retention_size);
    if (global_retention_acked) printf("Chính sách Retention Acked: Enabled\n");
    printf("Nhấn Ctrl + C để tắt server an toàn.\n\n");

    // Vòng lặp bây giờ phụ thuộc vào cờ keep_running thay vì while(1)
    while (keep_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        // Luồng chính sẽ bị block ở đây để chờ kết nối
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_len);
        
        if (client_socket < 0) {
            // Nếu accept() bị ngắt bởi tín hiệu Ctrl+C, mã lỗi sẽ là EINTR
            if (errno == EINTR) {
                break; // Thoát khỏi vòng lặp ngay lập tức
            }
            perror("Chấp nhận kết nối thất bại");
            continue;
        }

        int *pclient = malloc(sizeof(int));
        *pclient = client_socket;
        threadpool_add(pool, handle_client, pclient);
    }

    // --- QUÁ TRÌNH DỌN DẸP (CLEANUP) TRƯỚC KHI TẮT ---
    printf("\nĐang dọn dẹp tài nguyên mạng...\n");
    
    // 1. Đóng cổng lắng nghe để không nhận thêm request mới
    close(server_socket);
    printf("Đã đóng Server Socket.\n");

    // LƯU Ý CHO BÁO CÁO: 
    // Các file mmap() đã được munmap() an toàn ngay trong hàm storage_save_message().
    // Các client socket cũng đã được đóng gọn gàng trong luồng của handle_client().

    printf("Graceful Shutdown hoàn tất. Tạm biệt!\n");
    return 0;
}