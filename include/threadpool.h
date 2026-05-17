#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>

// Định nghĩa một công việc (Task) mà Worker sẽ thực thi
typedef struct {
    void (*function)(void *); // Con trỏ hàm xử lý client
    void *argument;           // Tham số truyền vào (ở đây là client_socket)
} threadpool_task_t;

// Cấu trúc quản lý Thread Pool
typedef struct {
    pthread_mutex_t lock;       // Khóa Mutex để chống Race Condition khi truy cập Queue
    pthread_cond_t notify;      // Tín hiệu đánh thức Worker khi có task mới
    pthread_t *threads;         // Mảng chứa các Worker threads
    threadpool_task_t *queue;   // Hàng đợi chứa các kết nối (Tasks)
    
    int thread_count;           // Tổng số threads
    int queue_size;             // Kích thước tối đa của hàng đợi
    int head;                   // Vị trí lấy task ra
    int tail;                   // Vị trí thêm task vào
    int count;                  // Số lượng task đang chờ trong Queue
} threadpool_t;

// Các hàm giao tiếp với Thread Pool
threadpool_t *threadpool_create(int thread_count, int queue_size);
int threadpool_add(threadpool_t *pool, void (*routine)(void *), void *arg);

#endif // THREADPOOL_H