#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "../include/threadpool.h"

// Vòng lặp vô tận của các Worker Threads
static void *threadpool_worker(void *threadpool) {
    threadpool_t *pool = (threadpool_t *)threadpool;

    while (1) {
        // 1. Khóa Mutex lại trước khi đụng vào Queue
        pthread_mutex_lock(&(pool->lock));

        // 2. Nếu Queue trống, thread sẽ đi ngủ (wait) để nhường CPU
        while (pool->count == 0) {
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        // 3. Lấy 1 task (client connection) ra khỏi Queue
        threadpool_task_t task = pool->queue[pool->head];
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count -= 1;

        // 4. Lấy xong rồi thì mở khóa Mutex cho thread khác vào lấy
        pthread_mutex_unlock(&(pool->lock));

        // 5. Thực thi công việc (xử lý giao tiếp với Client)
        (*(task.function))(task.argument);
    }
    return NULL;
}

// Hàm khởi tạo Thread Pool
threadpool_t *threadpool_create(int thread_count, int queue_size) {
    threadpool_t *pool = (threadpool_t *)malloc(sizeof(threadpool_t));
    
    pool->thread_count = thread_count;
    pool->queue_size = queue_size;
    pool->head = pool->tail = pool->count = 0;
    
    pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);
    pool->queue = (threadpool_task_t *)malloc(sizeof(threadpool_task_t) * queue_size);
    
    // Khởi tạo Mutex và Condition Variable
    pthread_mutex_init(&(pool->lock), NULL);
    pthread_cond_init(&(pool->notify), NULL);

    // Khởi tạo các Worker threads
    for (int i = 0; i < thread_count; i++) {
        pthread_create(&(pool->threads[i]), NULL, threadpool_worker, (void *)pool);
    }

    return pool;
}

// Hàm đẩy một Client connection vào Queue (Do Boss thread gọi)
int threadpool_add(threadpool_t *pool, void (*function)(void *), void *arg) {
    pthread_mutex_lock(&(pool->lock));

    // Nếu Queue đầy, từ chối nhận thêm
    if (pool->count == pool->queue_size) {
        pthread_mutex_unlock(&(pool->lock));
        return -1; 
    }

    // Thêm task vào Queue
    pool->queue[pool->tail].function = function;
    pool->queue[pool->tail].argument = arg;
    pool->tail = (pool->tail + 1) % pool->queue_size;
    pool->count += 1;

    // Đánh thức 1 Worker thread đang ngủ dậy làm việc
    pthread_cond_signal(&(pool->notify));
    pthread_mutex_unlock(&(pool->lock));

    return 0;
}