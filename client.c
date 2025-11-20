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
// You can run multiple clients (each in its own terminal). READ/WRITE/STREAM commands now connect
// directly to the appropriate Storage Server based on directions received from the Name Server.
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
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <stdarg.h>

#define NM_IP    "127.0.0.1"
#define NM_PORT  5000
#define ID_MAX   64
#define LINE_MAX 4096
#define FNAME_MAX 256
#define IP_MAX    64

// --- helpers ---
static void log_message(const char *level, const char *fmt, ...) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", t);

    fprintf(stderr, "[%s] [%s] ", time_str, level);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
}

static int connect_addr(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { 
        log_message("ERROR", "socket creation failed: %s", strerror(errno)); 
        return -1; 
    }
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        log_message("ERROR", "invalid ip: %s", ip); 
        close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_message("ERROR", "connect to %s:%d failed: %s", ip, port, strerror(errno));
        close(fd); return -1;
    }
    // log_message("DEBUG", "Connected to %s:%d", ip, port);
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
    bool printed_any = false;
    bool ended_with_newline = true;
    while ((n = recv(fd, buf, sizeof(buf)-1, 0)) > 0) {
        buf[n] = '\0';
        fputs(buf, stdout);
        fflush(stdout);
        printed_any = true;
        ended_with_newline = (buf[n-1] == '\n');
    }
    if (n < 0) {
        log_message("ERROR", "connection lost while receiving data: %s", strerror(errno));
    }
    if (printed_any && !ended_with_newline) {
        printf("\n");
        fflush(stdout);
    }
}

typedef struct {
    char verb[16];
    char filename[FNAME_MAX];
    char extra[64];
    char ss_id[ID_MAX];
    char ip[IP_MAX];
    int port;
} DirectEndpoint;

static bool request_direct_endpoint(const char *client_id, const char *cmdline, DirectEndpoint *out) {
    log_message("INFO", "Client %s requesting direct endpoint for command: %s", client_id, cmdline);
    int nmfd = connect_addr(NM_IP, NM_PORT);
    if (nmfd < 0) return false;
    dprintf(nmfd, "CLIENT %s %s\n", client_id, cmdline);
    char first_line[LINE_MAX];
    ssize_t got = read_line(nmfd, first_line, sizeof(first_line));
    if (got <= 0) {
        log_message("ERROR", "no response from Name Server");
        close(nmfd);
        return false;
    }
    if (strncmp(first_line, "DIRECT ", 7) != 0) {
        if (first_line[0] != '\0') {
            printf("%s\n", first_line);
        }
        read_all_and_print(nmfd);
        close(nmfd);
        return false;
    }
    DirectEndpoint tmp;
    if (sscanf(first_line, "DIRECT %15s %255s %63s %63s %63s %d",
               tmp.verb, tmp.filename, tmp.extra, tmp.ss_id, tmp.ip, &tmp.port) != 6) {
        log_message("ERROR", "malformed DIRECT response: %s", first_line);
        close(nmfd);
        return false;
    }
    log_message("INFO", "Received endpoint: SSID=%s IP=%s Port=%d for %s %s", tmp.ss_id, tmp.ip, tmp.port, tmp.verb, tmp.filename);
    *out = tmp;
    close(nmfd);
    return true;
}

static bool send_command_to_ss(const DirectEndpoint *ep, const char *client_id, const char *payload) {
    log_message("INFO", "Connecting to Storage Server %s (%s:%d)", ep->ss_id, ep->ip, ep->port);
    int ssfd = connect_addr(ep->ip, ep->port);
    if (ssfd < 0) {
        log_message("ERROR", "unable to connect to storage server %s (%s:%d)", ep->ss_id, ep->ip, ep->port);
        return false;
    }
    if (dprintf(ssfd, "HELLO %s %s\n", client_id, payload) < 0) {
        log_message("ERROR", "failed to send HELLO to SS: %s", strerror(errno));
        close(ssfd);
        return false;
    }
    read_all_and_print(ssfd);
    close(ssfd);
    return true;
}

static bool send_command_to_ss_with_response(const DirectEndpoint *ep, const char *client_id, const char *payload, char *first_line, size_t first_line_sz) {
    log_message("INFO", "Connecting to Storage Server %s (%s:%d) [Expect Response]", ep->ss_id, ep->ip, ep->port);
    int ssfd = connect_addr(ep->ip, ep->port);
    if (ssfd < 0) {
        log_message("ERROR", "unable to connect to storage server %s (%s:%d)", ep->ss_id, ep->ip, ep->port);
        return false;
    }
    if (dprintf(ssfd, "HELLO %s %s\n", client_id, payload) < 0) {
        log_message("ERROR", "failed to send HELLO to SS: %s", strerror(errno));
        close(ssfd);
        return false;
    }
    ssize_t n = read_line(ssfd, first_line, first_line_sz);
    if (n > 0 && first_line[0] != '\0') {
        printf("%s\n", first_line);
        // Check for error in response
        if (strncmp(first_line, "ERR", 3) == 0) {
             log_message("ERROR", "Storage Server returned error: %s", first_line);
        }
    }
    read_all_and_print(ssfd);
    close(ssfd);
    return (n > 0);
}

static void handle_direct_read(const char *client_id, const char *cmdline) {
    DirectEndpoint ep;
    if (!request_direct_endpoint(client_id, cmdline, &ep)) return;
    char payload[LINE_MAX];
    if (strcmp(ep.extra, "-") == 0) snprintf(payload, sizeof(payload), "READ %s", ep.filename);
    else snprintf(payload, sizeof(payload), "READ %s %s", ep.filename, ep.extra);
    send_command_to_ss(&ep, client_id, payload);
}

static void handle_direct_stream(const char *client_id, const char *cmdline) {
    DirectEndpoint ep;
    if (!request_direct_endpoint(client_id, cmdline, &ep)) return;
    char payload[LINE_MAX];
    snprintf(payload, sizeof(payload), "STREAM %s", ep.filename);
    send_command_to_ss(&ep, client_id, payload);
}

static void handle_direct_write(const char *client_id, const char *cmdline) {
    DirectEndpoint ep;
    if (!request_direct_endpoint(client_id, cmdline, &ep)) return;
    if (strcmp(ep.extra, "-") == 0) {
        log_message("ERROR", "WRITE missing sentence index");
        printf("ERROR WRITE missing sentence index\n");
        return;
    }

    char header[LINE_MAX];
    snprintf(header, sizeof(header), "WRITE %s %s", ep.filename, ep.extra);
    char response[LINE_MAX];
    if (!send_command_to_ss_with_response(&ep, client_id, header, response, sizeof(response))) {
        return;
    }

    // Check if the initial WRITE command failed
    if (strncmp(response, "ERR", 3) == 0 || strncmp(response, "STOP", 4) == 0) {
        // Error already printed by send_command_to_ss_with_response
        return;
    }

    printf("[Client %s] Enter <word_index> <content> lines. Finish with ETIRW.\n", client_id);
    char line[LINE_MAX];
    while (1) {
        printf("write> "); fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) {
            log_message("INFO", "EOF during WRITE; sending ETIRW to release lock.");
            printf("\n[Client %s] EOF during WRITE; sending ETIRW to release lock.\n", client_id);
            send_command_to_ss(&ep, client_id, "ETIRW");
            break;
        }
        size_t len = strlen(line);
        if (len && line[len-1] == '\n') line[len-1] = '\0';
        if (line[0] == '\0') continue;
        if (strcmp(line, "ETIRW") == 0) {
            send_command_to_ss(&ep, client_id, "ETIRW");
            break;
        }
        
        char update_response[LINE_MAX];
        if (send_command_to_ss_with_response(&ep, client_id, line, update_response, sizeof(update_response))) {
            // Check for STOP packet indicating error or completion
            if (strncmp(update_response, "STOP", 4) == 0) {
                log_message("INFO", "Received STOP from SS, ending write session.");
                break;
            }
        }
    }
}

// --- main ---
int main(void) {
    char client_id[ID_MAX] = {0 };

    printf("[Client] Enter your unique client id (username): ");
    fflush(stdout);
    if (scanf("%63s", client_id) != 1) {
        log_message("ERROR", "bad id input");
        fprintf(stderr, "bad id\n");
        return 1;
    }
    int ch; while ((ch = getchar()) != '\n' && ch != EOF) {} // clear line

    // Optional: introduce client to NM (so NM can maintain USER_LIST even before first command).
    {
        int fd = connect_addr(NM_IP, NM_PORT);
        if (fd >= 0) {
            log_message("INFO", "Introducing client %s to Name Server", client_id);
            dprintf(fd, "HELLOCLIENT %s\n", client_id);
            // Don't block on replies; NM may or may not send one. If it does, print it.
            char tmp[LINE_MAX];
            if (read_line(fd, tmp, sizeof(tmp)) > 0) {
                if (tmp[0] != '\0') { printf("%s\n", tmp); }
            }
            close(fd);
        } else {
            log_message("WARN", "Could not connect to Name Server for introduction");
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

        char verb_token[16]={0};
        sscanf(cmdline, "%15s", verb_token);
        if (strcasecmp(verb_token, "READ") == 0) {
            handle_direct_read(client_id, cmdline);
            continue;
        }
        if (strcasecmp(verb_token, "WRITE") == 0) {
            handle_direct_write(client_id, cmdline);
            continue;
        }
        if (strcasecmp(verb_token, "STREAM") == 0) {
            handle_direct_stream(client_id, cmdline);
            continue;
        }

        int nmfd = connect_addr(NM_IP, NM_PORT);
        if (nmfd < 0) { continue; }
        dprintf(nmfd, "CLIENT %s %s\n", client_id, cmdline);
        read_all_and_print(nmfd);
        close(nmfd);
    }
    return 0;
}
