// StorageServer.c
// Registers with Name Server, then listens for client connections.
// On each client connect, reads one HELLO line and logs the required debug message.

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define NM_IP   "127.0.0.1"
#define NM_PORT 5000
#define ID_MAX  64

static char g_storage_dir[PATH_MAX];

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

static void trim_spaces(char *s) {
    if (!s) return;
    char *start = s;
    while (*start && isspace((unsigned char)*start)) start++;
    char *end = start + strlen(start);
    while (end > start && isspace((unsigned char)end[-1])) end--;
    size_t len = (size_t)(end - start);
    if (start != s) memmove(s, start, len);
    s[len] = '\0';
}

static bool is_valid_name_component(const char *name) {
    if (!name || *name == '\0') return false;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return false;
    for (const unsigned char *p = (const unsigned char*)name; *p; ++p) {
        if (*p == '/' || *p == '\\') return false;
        if (isspace(*p)) return false;
        if (*p < 0x20) return false;
    }
    return true;
}

static int storage_path_for(const char *name, char *out, size_t out_sz) {
    if (!is_valid_name_component(name)) {
        errno = EINVAL;
        return -1;
    }
    if (g_storage_dir[0] == '\0') {
        errno = ENODEV;
        return -1;
    }
    int written = snprintf(out, out_sz, "%s/%s", g_storage_dir, name);
    if (written < 0 || (size_t)written >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int ensure_storage_directory(const char *ss_id) {
    if (!is_valid_name_component(ss_id)) {
        errno = EINVAL;
        return -1;
    }

    int written = snprintf(g_storage_dir, sizeof(g_storage_dir), "%s", ss_id);
    if (written < 0 || (size_t)written >= sizeof(g_storage_dir)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    struct stat st;
    if (stat(g_storage_dir, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) {
        return -1;
    }
    if (mkdir(g_storage_dir, 0777) < 0) {
        return -1;
    }
    return 0;
}

static int create_storage_file(const char *filename) {
    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        return -1;
    }
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd < 0) {
        return -1;
    }
    close(fd);
    return 0;
}

static int delete_storage_file(const char *filename) {
    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        return -1;
    }
    return unlink(path);
}

static int open_storage_file_ro(const char *filename) {
    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        return -1;
    }
    return open(path, O_RDONLY);
}

static int send_all(int fd, const void *buf, size_t len) {
    const unsigned char *ptr = (const unsigned char *)buf;
    while (len > 0) {
        ssize_t sent = send(fd, ptr, len, 0);
        if (sent < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        ptr += (size_t)sent;
        len -= (size_t)sent;
    }
    return 0;
}

int main(void) {
    char ss_id[ID_MAX];
    int client_port;

    printf("[SS] Enter storageserver_id: ");
    fflush(stdout);
    if (scanf("%63s", ss_id) != 1) { fprintf(stderr, "bad id\n"); return 1; }

    if (ensure_storage_directory(ss_id) != 0) {
        perror("storage directory");
        return 1;
    }

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
                char command_copy[LINE_MAX];
                strncpy(command_copy, cmd, sizeof(command_copy) - 1);
                command_copy[sizeof(command_copy) - 1] = '\0';
                trim_spaces(command_copy);
                if (command_copy[0] == '\0') {
                    dprintf(cfd, "ERR EMPTY_COMMAND\n");
                } else {
                    char *verb = command_copy;
                    char *arg = NULL;
                    for (char *p = command_copy; *p; ++p) {
                        if (isspace((unsigned char)*p)) {
                            *p = '\0';
                            arg = p + 1;
                            break;
                        }
                    }
                    if (arg) trim_spaces(arg);

                    char verb_upper[16];
                    size_t verb_len = strlen(verb);
                    if (verb_len >= sizeof(verb_upper)) verb_len = sizeof(verb_upper) - 1;
                    for (size_t i = 0; i < verb_len; ++i) {
                        verb_upper[i] = (char)toupper((unsigned char)verb[i]);
                    }
                    verb_upper[verb_len] = '\0';

                    if (strcmp(verb_upper, "CREATE") == 0) {
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR CREATE missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR CREATE invalid_filename\n");
                        } else {
                            int rc = create_storage_file(arg);
                            if (rc == 0) {
                                printf("[SS %s] created file %s for client %s\n", ss_id, arg, client_id);
                                fflush(stdout);
                                dprintf(cfd, "ACK %s CREATE OK %s\n", ss_id, arg);
                            } else {
                                int err = errno;
                                fprintf(stderr, "[SS %s] failed to create file %s for client %s: %s\n",
                                        ss_id, arg, client_id, strerror(err));
                                fflush(stderr);
                                if (err == EEXIST) {
                                    dprintf(cfd, "ERR CREATE EXISTS %s\n", arg);
                                } else {
                                    dprintf(cfd, "ERR CREATE %s\n", strerror(err));
                                }
                            }
                        }
                    } else if (strcmp(verb_upper, "DELETE") == 0) {
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR DELETE missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR DELETE invalid_filename\n");
                        } else {
                            if (delete_storage_file(arg) == 0) {
                                printf("[SS %s] deleted file %s on behalf of client %s\n", ss_id, arg, client_id);
                                fflush(stdout);
                                dprintf(cfd, "ACK %s DELETE OK %s\n", ss_id, arg);
                            } else {
                                int err = errno;
                                fprintf(stderr, "[SS %s] failed to delete file %s for client %s: %s\n",
                                        ss_id, arg, client_id, strerror(err));
                                fflush(stderr);
                                if (err == ENOENT) {
                                    dprintf(cfd, "ERR DELETE NOTFOUND %s\n", arg);
                                } else {
                                    dprintf(cfd, "ERR DELETE %s\n", strerror(err));
                                }
                            }
                        }
                    } else if (strcmp(verb_upper, "READ") == 0) {
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR READ missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR READ invalid_filename\n");
                        } else {
                            int fd = open_storage_file_ro(arg);
                            if (fd < 0) {
                                int err = errno;
                                fprintf(stderr, "[SS %s] failed to open file %s for client %s: %s\n",
                                        ss_id, arg, client_id, strerror(err));
                                fflush(stderr);
                                if (err == ENOENT) {
                                    dprintf(cfd, "ERR READ NOTFOUND %s\n", arg);
                                } else {
                                    dprintf(cfd, "ERR READ %s\n", strerror(err));
                                }
                            } else {
                                struct stat st;
                                long long declared_size = -1;
                                if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) {
                                    declared_size = st.st_size;
                                }
                                dprintf(cfd, "DATA %s READ %s %lld\n", ss_id, arg, declared_size);

                                char buffer[4096];
                                int send_failed = 0;
                                ssize_t rbytes;
                                while ((rbytes = read(fd, buffer, sizeof(buffer))) > 0) {
                                    if (send_all(cfd, buffer, (size_t)rbytes) < 0) {
                                        send_failed = 1;
                                        int err = errno;
                                        fprintf(stderr, "[SS %s] send failure while streaming %s to client %s: %s\n",
                                                ss_id, arg, client_id, strerror(err));
                                        fflush(stderr);
                                        break;
                                    }
                                }
                                if (rbytes < 0) {
                                    int err = errno;
                                    fprintf(stderr, "[SS %s] read failure while streaming %s to client %s: %s\n",
                                            ss_id, arg, client_id, strerror(err));
                                    fflush(stderr);
                                    if (!send_failed) {
                                        dprintf(cfd, "\nERR READ IO %s\n", strerror(err));
                                    }
                                } else if (!send_failed) {
                                    dprintf(cfd, "\nENDDATA %s READ %s\n", ss_id, arg);
                                    printf("[SS %s] streamed file %s to client %s\n", ss_id, arg, client_id);
                                    fflush(stdout);
                                }
                                close(fd);
                            }
                        }
                    } else {
                        dprintf(cfd, "ERR UNKNOWN_COMMAND %s\n", verb);
                    }
                }
            }
        }
        close(cfd);
    }
    return 0;
}
