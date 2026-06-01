#ifndef NETWORK_H
#define NETWORK_H

#include "protocol.h"

int create_server_socket(int port);
ssize_t read_exact(int fd, void *buf, size_t n);
Message *receive_message(int client_socket);
void free_message(Message *msg);

#endif // NETWORK_H