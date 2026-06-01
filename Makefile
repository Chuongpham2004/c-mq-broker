# Trình biên dịch và các cờ (flags)
CC = gcc
CFLAGS = -Wall -Wextra -pthread -g -std=c11
BIN_DIR = bin

# Rule mặc định khi gõ 'make'
all: broker producer consumer noack_client

# Rule build Broker
broker: src/server.c src/network.c src/threadpool.c src/broker.c src/storage.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $(BIN_DIR)/$@
	@echo "Đã build xong Broker!"

# Rule build Producer
producer: clients/producer.c src/network.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $(BIN_DIR)/$@
	@echo "Đã build xong Producer!"

# Rule build Consumer
consumer: clients/consumer.c src/network.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $(BIN_DIR)/$@
	@echo "Đã build xong Consumer!"

# Rule build NoACK Client
noack_client: clients/noack_client.c src/network.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $^ -o $(BIN_DIR)/$@
	@echo "Đã build xong NoACK Client!"

# Rule chạy test suite
test: broker producer consumer noack_client
	@chmod +x test_suite.sh
	@./test_suite.sh

# Rule dọn dẹp file thực thi khi gõ 'make clean'
clean:
	rm -rf $(BIN_DIR)
	@echo "Đã dọn dẹp thư mục build!"