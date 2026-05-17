#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "../include/storage.h"

#define DATA_DIR "./data"

// Hàm tạo thư mục data/ nếu chưa tồn tại
void init_storage() {
    struct stat st = {0};
    if (stat(DATA_DIR, &st) == -1) {
        mkdir(DATA_DIR, 0700); // Cấp quyền rwx------
        printf("[Storage] Đã tạo thư mục lưu trữ: %s\n", DATA_DIR);
    }
}

// Hàm ghi bản tin vào file bằng mmap
void storage_save_message(const char *topic, Message *msg) {
    if (!topic || !msg) return;

    // 1. Xử lý tên file (thay dấu '/' thành '_' để không bị lỗi đường dẫn)
    // Ví dụ: topic "sensor/nhietdo" -> file "data/sensor_nhietdo.log"
    char filepath[512];
    char safe_topic[256];
    strncpy(safe_topic, topic, 255);
    for(int i = 0; safe_topic[i]; i++) {
        if(safe_topic[i] == '/') safe_topic[i] = '_';
    }
    snprintf(filepath, sizeof(filepath), "%s/%s.log", DATA_DIR, safe_topic);

    // 2. Mở file (nếu chưa có thì tạo mới)
    int fd = open(filepath, O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        perror("[Storage] Lỗi mở file");
        return;
    }

    // 3. Tính toán kích thước của gói tin cần ghi
    size_t record_size = sizeof(MessageHeader) + msg->header.topic_len + msg->header.payload_len;

    // Lấy kích thước file hiện tại
    struct stat st;
    fstat(fd, &st);
    off_t old_size = st.st_size;
    off_t new_size = old_size + record_size;

    // 4. Mở rộng kích thước file để có chỗ nhét thêm data (ftruncate)
    if (ftruncate(fd, new_size) == -1) {
        perror("[Storage] Lỗi ftruncate");
        close(fd);
        return;
    }

    // 5. Ánh xạ (mmap) file vào bộ nhớ RAM
    // PROT_READ | PROT_WRITE: Cho phép đọc ghi
    // MAP_SHARED: Đồng bộ thay đổi xuống file gốc
    void *map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        perror("[Storage] Lỗi mmap");
        close(fd);
        return;
    }

    // 6. Ghi dữ liệu vào RAM (Hệ điều hành sẽ lo việc ghi xuống đĩa)
    // Di chuyển con trỏ đến vị trí cuối file cũ
    char *ptr = (char *)map + old_size;

    memcpy(ptr, &msg->header, sizeof(MessageHeader));
    ptr += sizeof(MessageHeader);
    
    if (msg->header.topic_len > 0) {
        memcpy(ptr, msg->topic, msg->header.topic_len);
        ptr += msg->header.topic_len;
    }
    
    if (msg->header.payload_len > 0) {
        memcpy(ptr, msg->payload, msg->header.payload_len);
    }

    // 7. Ép hệ thống đồng bộ RAM xuống đĩa cứng ngay lập tức (msync)
    msync(map, new_size, MS_SYNC);

    // 8. Giải phóng vùng map và đóng file
    munmap(map, new_size);
    close(fd);
    
    printf("[Storage] Đã lưu thành công bản tin (%zu bytes) vào %s\n", record_size, filepath);
}