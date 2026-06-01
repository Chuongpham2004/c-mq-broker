#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include "../include/storage.h"

#include <time.h>

#define DATA_DIR "./data"

FsyncPolicy global_fsync_policy = FSYNC_PER_WRITE;
pthread_mutex_t storage_mutex = PTHREAD_MUTEX_INITIALIZER;

// Hàm tạo thư mục data/ nếu chưa tồn tại
void init_storage() {
    struct stat st = {0};
    if (stat(DATA_DIR, &st) == -1) {
        mkdir(DATA_DIR, 0700); // Cấp quyền rwx------
        printf("[Storage] Đã tạo thư mục lưu trữ: %s\n", DATA_DIR);
    }
}

// Hàm ghi bản tin vào file bằng mmap
size_t storage_save_message(const char *topic, Message *msg) {
    if (!topic || !msg) return 0;

    pthread_mutex_lock(&storage_mutex);

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
        pthread_mutex_unlock(&storage_mutex);
        return 0;
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
        pthread_mutex_unlock(&storage_mutex);
        return 0;
    }

    // 5. Ánh xạ (mmap) file vào bộ nhớ RAM
    // PROT_READ | PROT_WRITE: Cho phép đọc ghi
    // MAP_SHARED: Đồng bộ thay đổi xuống file gốc
    void *map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        perror("[Storage] Lỗi mmap");
        close(fd);
        pthread_mutex_unlock(&storage_mutex);
        return 0;
    }

    // 6. Ghi dữ liệu vào RAM (Hệ điều hành sẽ lo việc ghi xuống đĩa)
    // Di chuyển con trỏ đến vị trí cuối file cũ
    char *ptr = (char *)map + old_size;

    // Chuyển đổi các trường header sang Network Byte Order trước khi ghi xuống đĩa
    MessageHeader disk_header = msg->header;
    disk_header.msg_id = htonl(disk_header.msg_id);
    disk_header.timestamp = htonl(disk_header.timestamp);
    disk_header.payload_len = htonl(disk_header.payload_len);

    memcpy(ptr, &disk_header, sizeof(MessageHeader));
    ptr += sizeof(MessageHeader);
    
    if (msg->header.topic_len > 0) {
        memcpy(ptr, msg->topic, msg->header.topic_len);
        ptr += msg->header.topic_len;
    }
    
    if (msg->header.payload_len > 0) {
        memcpy(ptr, msg->payload, msg->header.payload_len);
    }

    // 7. Ép hệ thống đồng bộ RAM xuống đĩa cứng theo chính sách fsync cấu hình
    static int write_count = 0;
    static time_t last_sync_time = 0;
    if (last_sync_time == 0) last_sync_time = time(NULL);
    
    int do_sync = 0;
    if (global_fsync_policy == FSYNC_PER_WRITE) {
        do_sync = 1;
    } else if (global_fsync_policy == FSYNC_GROUP_COMMIT) {
        write_count++;
        if (write_count >= 5) {
            do_sync = 1;
            write_count = 0;
        }
    } else if (global_fsync_policy == FSYNC_TIME_BASED) {
        time_t now = time(NULL);
        if (now - last_sync_time >= 1) {
            do_sync = 1;
            last_sync_time = now;
        }
    }
    
    if (do_sync) {
        msync(map, new_size, MS_SYNC);
    } else {
        msync(map, new_size, MS_ASYNC);
    }

    // 8. Giải phóng vùng map và đóng file
    munmap(map, new_size);
    close(fd);
    
    printf("[Storage] Đã lưu thành công bản tin (%zu bytes) vào %s\n", record_size, filepath);
    pthread_mutex_unlock(&storage_mutex);
    return new_size;
}

// Hàm cắt bỏ phần đầu file log để giải phóng không gian
void trim_log_file(const char *filepath, size_t bytes_to_remove) {
    if (bytes_to_remove == 0) return;

    pthread_mutex_lock(&storage_mutex);

    int fd = open(filepath, O_RDWR);
    if (fd == -1) {
        pthread_mutex_unlock(&storage_mutex);
        return;
    }
    
    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        pthread_mutex_unlock(&storage_mutex);
        return;
    }
    
    if ((size_t)st.st_size <= bytes_to_remove) {
        if (ftruncate(fd, 0) == -1) {
            perror("[Storage] Lỗi ftruncate khi xóa toàn bộ file");
        }
        close(fd);
        pthread_mutex_unlock(&storage_mutex);
        return;
    }
    
    size_t new_size = st.st_size - bytes_to_remove;
    void *map = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map != MAP_FAILED) {
        // Dịch chuyển phần dữ liệu phía sau lên đầu file
        memmove(map, (char *)map + bytes_to_remove, new_size);
        msync(map, new_size, MS_SYNC);
        munmap(map, st.st_size);
    } else {
        perror("[Storage] Lỗi mmap khi trim file");
    }
    
    if (ftruncate(fd, new_size) == -1) {
        perror("[Storage] Lỗi ftruncate sau khi trim file");
    }
    close(fd);
    printf("[Storage] Đã trim bớt %zu bytes từ file %s, kích thước mới: %zu bytes\n", bytes_to_remove, filepath, new_size);
    pthread_mutex_unlock(&storage_mutex);
}