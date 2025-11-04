// Client.c (Step-2)
// - Prompts for client unique id (username).
// - Registers with Name Server.
// - For each command typed, sends it to the Name Server and prints the entire response.
// - Special handling for multi-line WRITE ... ETIRW: forwards every subsequent line to NM on the
//   same socket until ETIRW is seen, then prints NM's response.
//
// Protocol notes (simple & text-based):
// * For single-line commands (VIEW/CREATE/READ/UNDO/DELETE/STREAM/LIST/ADDACCESS/REMACCESS/EXEC/etc.),
//   we open a fresh TCP connection to the NM, send one line, then read till EOF and print everything.
// * For WRITE, we open one connection, send "WRITE <file> <sentence_idx>" once,
//   then forward every user line verbatim until "ETIRW", then read till EOF and print.
//
// You can run multiple clients (each in its own terminal). This file does not connect to storage
// servers directly in Step-2; the Name Server handles routing/proxying.
//
// Build: gcc -o Client Client.c
// Run:   ./Client

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

#define NM_IP    "127.0.0.1"
#define NM_PORT  5000
#define ID_MAX   64
#define LINE_MAX 4096

// --- helpers ---
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
        if (r == 0) break;              // EOF
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (c == '\r') continue;        // ignore CR
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = '\0';
    return (ssize_t)n;
}

static void read_all_and_print(int fd) {
    // Print everything the NM sends until it closes the socket.
    char buf[LINE_MAX];
    ssize_t n;
    while ((n = recv(fd, buf, sizeof(buf)-1, 0)) > 0) {
        buf[n] = '\0';
        fputs(buf, stdout);
        fflush(stdout);
    }
}

static bool has_prefix(const char *s, const char *pfx) {
    return strncmp(s, pfx, strlen(pfx)) == 0;
}

// --- main ---
int main(void) {
    char client_id[ID_MAX] = {0};

    printf("[Client] Enter your unique client id (username): ");
    fflush(stdout);
    if (scanf("%63s", client_id) != 1) {
        fprintf(stderr, "bad id\n");
        return 1;
    }
    int ch; while ((ch = getchar()) != '\n' && ch != EOF) {} // clear line

    // Optional: introduce client to NM (so NM can maintain USER_LIST even before first command).
    {
        int fd = connect_addr(NM_IP, NM_PORT);
        if (fd >= 0) {
            dprintf(fd, "HELLOCLIENT %s\n", client_id);
            // Don't block on replies; NM may or may not send one. If it does, print it.
            char tmp[LINE_MAX];
            if (read_line(fd, tmp, sizeof(tmp)) > 0) {
                if (tmp[0] != '\0') { printf("%s\n", tmp); }
            }
            close(fd);
        }
    }

    printf("[Client %s] Type commands. For WRITE, enter lines until ETIRW.\n", client_id);
    printf("Exit with EOF (Ctrl+D) or type QUIT/EXIT.\n");
    fflush(stdout);

    char cmdline[LINE_MAX];
    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(cmdline, sizeof(cmdline), stdin)) {
            printf("\n[Client %s] Bye.\n", client_id);
            break;
        }
        // strip newline
        size_t len = strlen(cmdline);
        if (len && cmdline[len-1] == '\n') cmdline[len-1] = '\0';
        if (cmdline[0] == '\0') continue;

        if (strcasecmp(cmdline, "QUIT") == 0 || strcasecmp(cmdline, "EXIT") == 0) {
            printf("[Client %s] Bye.\n", client_id);
            break;
        }

        // Open one connection per *command*.
        int nmfd = connect_addr(NM_IP, NM_PORT);
        if (nmfd < 0) { continue; }

        // Always prefix client id so NM can authz/audit
        // (NM can accept either "REQUEST <id> <payload>" or direct payloads that begin with verbs.)
        // We use a simple unified line: CLIENT <id> <payload>
        if (has_prefix(cmdline, "WRITE ")) {
            // 1) send the first WRITE line
            dprintf(nmfd, "CLIENT %s %s\n", client_id, cmdline);

            // 2) now forward subsequent user lines until ETIRW
            while (1) {
                char line[LINE_MAX];
                if (!fgets(line, sizeof(line), stdin)) {
                    // EOF mid-write: still let NM handle it (socket will close from our side)
                    break;
                }
                size_t l = strlen(line);
                if (l && line[l-1] == '\n') line[l-1] = '\0';

                // Echo user typing? optional; keep quiet.
                dprintf(nmfd, "%s\n", line);

                if (strcmp(line, "ETIRW") == 0) {
                    // done sending; read server output till EOF
                    break;
                }
            }

            // 3) print everything NM returns (status/errors/redirect text/streamed result)
            read_all_and_print(nmfd);
            close(nmfd);
        } else {
            // Single-line commands: forward line and print all output till EOF
            dprintf(nmfd, "CLIENT %s %s\n", client_id, cmdline);
            read_all_and_print(nmfd);
            close(nmfd);
        }
    }
    return 0;
}
