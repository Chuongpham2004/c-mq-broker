#ifndef STORAGE_H
#define STORAGE_H

#include "protocol.h"
#include <pthread.h>

typedef enum {
    FSYNC_PER_WRITE,
    FSYNC_GROUP_COMMIT,
    FSYNC_TIME_BASED
} FsyncPolicy;

extern FsyncPolicy global_fsync_policy;
extern pthread_mutex_t storage_mutex;

// Khởi tạo thư mục chứa data
void init_storage();

// Lưu một bản tin vào đĩa cứng (dùng mmap), trả về kích thước mới của file (offset cuối cùng)
size_t storage_save_message(const char *topic, Message *msg);

// Cắt gọn file log
void trim_log_file(const char *filepath, size_t bytes_to_remove);

#endif // STORAGE_H