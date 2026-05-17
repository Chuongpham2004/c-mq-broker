#ifndef STORAGE_H
#define STORAGE_H

#include "protocol.h"

// Khởi tạo thư mục chứa data
void init_storage();

// Lưu một bản tin vào đĩa cứng (dùng mmap)
void storage_save_message(const char *topic, Message *msg);

#endif // STORAGE_H