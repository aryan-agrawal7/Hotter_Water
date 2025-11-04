// NamedServer.c
// Minimal threaded Name Server for OSN "connection-only" step.
// - Accepts Storage Server registrations:  REGISTER <ss_id> <client_port>
// - Handles Client requests:               REQUEST <client_id> <command...>
// - Replies: CONNECT <ss_id> <ip> <port>
// - Round-robin selection among registered Storage Servers.

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define NM_PORT 5000
#define MAX_SS 256
#define ID_MAX 64
#define IP_MAX INET_ADDRSTRLEN
#define LINE_MAX 2048

typedef struct {
    char id[ID_MAX];
    char ip[IP_MAX];
    int  client_port;
    bool in_use;
} StorageServerInfo;

static StorageServerInfo g_ss[MAX_SS];
static int g_ss_count = 0;
static int g_rr_index = 0;
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

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

static ssize_t read_line(int fd, char *buf, size_t maxlen) {
    size_t n = 0; char c;
    while (n + 1 < maxlen) {
        ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0) break;          // EOF
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (c == '\r') continue;    // ignore CR
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = '\0';
    return (ssize_t)n;
}

static void get_peer_ip(int fd, char out_ip[IP_MAX]) {
    struct sockaddr_in addr; socklen_t len = sizeof(addr);
    if (getpeername(fd, (struct sockaddr*)&addr, &len) == 0) {
        inet_ntop(AF_INET, &addr.sin_addr, out_ip, IP_MAX);
    } else {
        strncpy(out_ip, "127.0.0.1", IP_MAX);
    }
}

static void register_ss(const char *id, int client_port, const char *peer_ip) {
    pthread_mutex_lock(&g_mtx);
    // If id already exists, update; else append.
    for (int i = 0; i < g_ss_count; ++i) {
        if (g_ss[i].in_use && strncmp(g_ss[i].id, id, ID_MAX) == 0) {
            strncpy(g_ss[i].ip, peer_ip, IP_MAX);
            g_ss[i].client_port = client_port;
            pthread_mutex_unlock(&g_mtx);
            printf("[NM] updated storage server %s at %s:%d\n", id, peer_ip, client_port);
            fflush(stdout);
            return;
        }
    }
    if (g_ss_count < MAX_SS) {
        StorageServerInfo *slot = &g_ss[g_ss_count++];
        memset(slot, 0, sizeof(*slot));
        strncpy(slot->id, id, ID_MAX-1);
        strncpy(slot->ip, peer_ip, IP_MAX-1);
        slot->client_port = client_port;
        slot->in_use = true;
        printf("[NM] registered storage server %s at %s:%d (total=%d)\n",
               id, peer_ip, client_port, g_ss_count);
        fflush(stdout);
    } else {
        fprintf(stderr, "[NM] registry full; cannot register %s\n", id);
        fflush(stderr);
    }
    pthread_mutex_unlock(&g_mtx);
}

static bool pick_storage_server(StorageServerInfo *out) {
    pthread_mutex_lock(&g_mtx);
    if (g_ss_count == 0) { pthread_mutex_unlock(&g_mtx); return false; }
    // Simple round-robin on the in_use entries
    for (int tries = 0; tries < g_ss_count; ++tries) {
        int idx = g_rr_index % g_ss_count;
        g_rr_index = (g_rr_index + 1) % (g_ss_count == 0 ? 1 : g_ss_count);
        if (g_ss[idx].in_use) {
            *out = g_ss[idx];
            pthread_mutex_unlock(&g_mtx);
            return true;
        }
    }
    pthread_mutex_unlock(&g_mtx);
    return false;
}

typedef struct { int fd; } WorkerArg;

static void *worker(void *arg_) {
    WorkerArg *wa = (WorkerArg*)arg_;
    int fd = wa->fd;
    free(wa);

    char line[LINE_MAX];
    ssize_t n = read_line(fd, line, sizeof(line));
    if (n <= 0) { close(fd); return NULL; }

    if (strncmp(line, "REGISTER", 8) == 0) {
        char ss_id[ID_MAX]; int client_port = 0;
        if (sscanf(line, "REGISTER %63s %d", ss_id, &client_port) == 2) {
            char ip[IP_MAX]; get_peer_ip(fd, ip);
            register_ss(ss_id, client_port, ip);
            dprintf(fd, "OK\n");
        } else {
            dprintf(fd, "ERROR malformed REGISTER\n");
        }
    } else if (strncmp(line, "REQUEST", 7) == 0) {
        char client_id[ID_MAX]; char cmd[LINE_MAX];
        client_id[0] = '\0'; cmd[0] = '\0';
        // Grab client id and the rest of the command as free text
        if (sscanf(line, "REQUEST %63s %2047[^\n]", client_id, cmd) >= 1) {
            if (cmd[0] == '\0') strcpy(cmd, "(empty)");
            printf("[NM] received request %s from client with unique id %s\n", cmd, client_id);
            fflush(stdout);
            StorageServerInfo choice;
            if (pick_storage_server(&choice)) {
                dprintf(fd, "CONNECT %s %s %d\n", choice.id, choice.ip, choice.client_port);
            } else {
                dprintf(fd, "ERROR NO_STORAGE_SERVERS\n");
            }
        } else {
            dprintf(fd, "ERROR malformed REQUEST\n");
        }
    } else {
        dprintf(fd, "ERROR unknown command\n");
    }
    close(fd);
    return NULL;
}

int main(void) {
    int listen_fd = create_listen_socket(NM_PORT);
    printf("[NM] listening on port %d\n", NM_PORT);
    fflush(stdout);

    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept"); continue;
        }
        WorkerArg *wa = malloc(sizeof(*wa));
        wa->fd = cfd;
        pthread_t th;
        if (pthread_create(&th, NULL, worker, wa) == 0) {
            pthread_detach(th);
        } else {
            perror("pthread_create");
            close(cfd);
            free(wa);
        }
    }
    return 0;
}
