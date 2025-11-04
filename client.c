// Client.c
// Prompts for client unique id. For each terminal command (e.g., "VIEW -a"),
// sends REQUEST to Name Server, receives CONNECT <ss_id> <ip> <port>,
// then connects to that Storage Server and says HELLO.

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

static int connect_addr(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "invalid ip: %s\n", ip); close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect"); close(fd); return -1;
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
    char client_id[ID_MAX];
    printf("[Client] Enter your unique client id: ");
    fflush(stdout);
    if (scanf("%63s", client_id) != 1) { fprintf(stderr, "bad id\n"); return 1; }
    // Drain trailing newline
    int ch; while ((ch = getchar()) != '\n' && ch != EOF) {}

    printf("[Client %s] Type commands to send to Name Server (e.g., VIEW -a). Type EOF (Ctrl+D) to exit.\n", client_id);
    fflush(stdout);

    char cmdline[LINE_MAX];
    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(cmdline, sizeof(cmdline), stdin)) break;
        // Strip trailing newline
        size_t len = strlen(cmdline);
        if (len && cmdline[len-1] == '\n') cmdline[len-1] = '\0';
        if (cmdline[0] == '\0') continue;

        // 1) Send request to Name Server
        int nmfd = connect_addr(NM_IP, NM_PORT);
        if (nmfd < 0) continue;
        dprintf(nmfd, "REQUEST %s %s\n", client_id, cmdline);

        char reply[LINE_MAX];
        if (read_line(nmfd, reply, sizeof(reply)) <= 0) {
            fprintf(stderr, "[Client] No reply from Name Server\n");
            close(nmfd); continue;
        }
        close(nmfd);

        // Expect: CONNECT <ss_id> <ip> <port>
        char ss_id[ID_MAX], ip[64]; int port = 0;
        if (sscanf(reply, "CONNECT %63s %63s %d", ss_id, ip, &port) == 3) {
            printf("[Client %s] NM selected storage server %s at %s:%d\n", client_id, ss_id, ip, port);
            // 2) Connect to Storage Server
            int ssfd = connect_addr(ip, port);
            if (ssfd < 0) { continue; }
            dprintf(ssfd, "HELLO %s %s\n", client_id, cmdline);

            // Optionally wait for ACK
            char ack[LINE_MAX];
            if (read_line(ssfd, ack, sizeof(ack)) > 0) {
                printf("[Client %s] SS replied: %s\n", client_id, ack);
            } else {
                printf("[Client %s] Connected to SS and sent HELLO.\n", client_id);
            }
            close(ssfd);
        } else {
            fprintf(stderr, "[Client] NM error: %s\n", reply);
        }
    }

    printf("[Client %s] Bye.\n", client_id);
    return 0;
}
