// StorageServer.c
// Registers with Name Server, then listens for client connections.
// On each client connect, reads one HELLO line and logs the required debug message.

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define NM_IP   "127.0.0.1"
#define NM_PORT 5000
#define ID_MAX  64
#define LINE_MAX 2048

static int create_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); exit(1); }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }
    if (listen(fd, 128) < 0) { perror("listen"); exit(1); }
    return fd;
}

static int connect_to_nm(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); exit(1); }
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid NM ip\n"); exit(1);
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect NM"); exit(1);
    }
    return fd;
}

static ssize_t read_line(int fd, char *buf, size_t maxlen) {
    size_t n = 0; char c;
    while (n + 1 < maxlen) {
        ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0) break;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (c == '\r') continue;
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = '\0';
    return (ssize_t)n;
}

int main(void) {
    char ss_id[ID_MAX];
    int client_port;

    printf("[SS] Enter storageserver_id: ");
    fflush(stdout);
    if (scanf("%63s", ss_id) != 1) { fprintf(stderr, "bad id\n"); return 1; }

    printf("[SS] Enter client_port to listen on (e.g., 6001): ");
    fflush(stdout);
    if (scanf("%d", &client_port) != 1) { fprintf(stderr, "bad port\n"); return 1; }

    // Drain leftover newline from stdin so later fgets/reads aren't polluted
    int ch; while ((ch = getchar()) != '\n' && ch != EOF) {}

    int listen_fd = create_listen_socket(client_port);
    printf("[SS %s] listening for clients on port %d\n", ss_id, client_port);
    fflush(stdout);

    // Register with Name Server
    int nmfd = connect_to_nm(NM_IP, NM_PORT);
    dprintf(nmfd, "REGISTER %s %d\n", ss_id, client_port);
    char resp[LINE_MAX]; read_line(nmfd, resp, sizeof(resp));
    printf("[SS %s] registration reply from NM: %s\n", ss_id, resp);
    close(nmfd);

    // Accept clients and print debug on connect
    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept"); continue;
        }
        char line[LINE_MAX];
        ssize_t n = read_line(cfd, line, sizeof(line));
        if (n > 0) {
            // Expect: HELLO <client_id> <command...>
            char client_id[ID_MAX] = {0};
            char cmd[LINE_MAX] = {0};
            if (sscanf(line, "HELLO %63s %2047[^\n]", client_id, cmd) >= 1) {
                printf("storage server %s connected successfully to client %s\n", ss_id, client_id);
                fflush(stdout);
                // Optional: echo or ACK
                dprintf(cfd, "ACK %s\n", ss_id);
            }
        }
        close(cfd);
    }
    return 0;
}
