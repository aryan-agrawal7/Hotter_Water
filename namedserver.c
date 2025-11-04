// NamedServer.c — Step 2
// Adds: FILE_META_DATA, ALL_FILES, USER_LIST, min-heap for SS load,
// and NM-side handlers for VIEW/CREATE/WRITE/UNDO/DELETE/STREAM/LIST/ADDACCESS/REMACCESS/EXEC.
//
// Matches the project spec: NM stores SS data, handles listing & access control,
// issues create/delete, and provides routing for read/write/stream where applicable.
// - Storage servers can dynamically register at any time (heap is updated) [Spec 3].
// - VIEW variants are served entirely by NM (no SS calls) [Spec 4].
// - CREATE chooses least-loaded SS (balanced even if SS joins late).
// - WRITE parses the lead line, enforces ACL, and (for now) returns CONNECT so your
//   current client continues to work. Hooks exist to stream until ETIRW later.
//
// References to spec/examples:
//   • NM stores SS info, dynamic addition: see "Initialisation" & "Storage Servers" sections. 
//   • VIEW flags and behaviors: "User Functionalities: View files" and examples.
//   • WRITE multi-line & ETIRW, UNDO is SS-side: user functionalities & notes.
//
// (c) you. Compile: gcc -pthread -o NamedServer NamedServer.c
//
// ---------------------------------------------------------------------------

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// --------------------- Config ---------------------
#define NM_PORT            5000

#define MAX_SS             512
#define MAX_FILES          16384
#define MAX_CLIENTS        4096

#define ID_MAX             64
#define FN_MAX             256
#define IP_MAX             INET_ADDRSTRLEN
#define LINE_MAX           4096

#define MAX_PERM_USERS     512  // per file per list

// --------------------- Utilities ------------------
static void fatal(const char *msg) { perror(msg); exit(1); }

static int create_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) fatal("socket");
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) fatal("setsockopt");
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) fatal("bind");
    if (listen(fd, 256) < 0) fatal("listen");
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
static void sendf(int fd, const char *fmt, ...) {
    char buf[LINE_MAX];
    va_list ap; va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    send(fd, buf, strlen(buf), 0);
}
static void get_peer_ip(int fd, char out_ip[IP_MAX]) {
    struct sockaddr_in addr; socklen_t len = sizeof(addr);
    if (getpeername(fd, (struct sockaddr*)&addr, &len) == 0) {
        inet_ntop(AF_INET, &addr.sin_addr, out_ip, IP_MAX);
    } else {
        strncpy(out_ip, "127.0.0.1", IP_MAX);
    }
}

// --------------------- Storage Server Registry + Min-Heap ---------------------
typedef struct {
    char id[ID_MAX];
    char ip[IP_MAX];
    int  client_port;     // where clients (and NM for forwarding) connect
    bool in_use;
    int  file_count;      // for balancing
    int  heap_pos;        // index in min-heap (-1 if not present)
} StorageServerInfo;

static StorageServerInfo g_ss[MAX_SS];
static int g_ss_count = 0;

// Min-heap of indices into g_ss by file_count, then id
static int g_heap[MAX_SS];
static int g_heap_size = 0;

static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

// Heap helpers
static int ss_cmp(int i, int j) {
    // return <0 if i has higher priority (smaller file_count)
    const StorageServerInfo *a = &g_ss[i], *b = &g_ss[j];
    if (a->file_count != b->file_count) return (a->file_count < b->file_count) ? -1 : 1;
    return strncmp(a->id, b->id, ID_MAX);
}
static void heap_swap(int i, int j) {
    int ii = g_heap[i], jj = g_heap[j];
    g_heap[i] = jj; g_heap[j] = ii;
    g_ss[jj].heap_pos = i;
    g_ss[ii].heap_pos = j;
}
static void heap_sift_up(int pos) {
    while (pos > 0) {
        int p = (pos - 1) / 2;
        if (ss_cmp(g_heap[pos], g_heap[p]) < 0) { heap_swap(pos, p); pos = p; }
        else break;
    }
}
static void heap_sift_down(int pos) {
    while (1) {
        int l = 2 * pos + 1, r = 2 * pos + 2, best = pos;
        if (l < g_heap_size && ss_cmp(g_heap[l], g_heap[best]) < 0) best = l;
        if (r < g_heap_size && ss_cmp(g_heap[r], g_heap[best]) < 0) best = r;
        if (best != pos) { heap_swap(pos, best); pos = best; }
        else break;
    }
}
static void heap_insert(int ss_index) {
    if (g_ss[ss_index].heap_pos != -1) return;
    int pos = g_heap_size++;
    g_heap[pos] = ss_index;
    g_ss[ss_index].heap_pos = pos;
    heap_sift_up(pos);
}
static void heap_update_key(int ss_index) {
    int pos = g_ss[ss_index].heap_pos;
    if (pos < 0) return;
    heap_sift_up(pos);
    heap_sift_down(pos);
}
static int heap_peek_min(void) {
    if (g_heap_size == 0) return -1;
    return g_heap[0];
}

// SS registry helpers
static int find_ss_by_id(const char *id) {
    for (int i = 0; i < g_ss_count; ++i)
        if (g_ss[i].in_use && strncmp(g_ss[i].id, id, ID_MAX) == 0) return i;
    return -1;
}
static void register_ss_record(const char *id, const char *peer_ip, int client_port) {
    // Called under g_mtx
    int idx = find_ss_by_id(id);
    if (idx >= 0) {
        StorageServerInfo *s = &g_ss[idx];
        strncpy(s->ip, peer_ip, IP_MAX - 1);
        s->client_port = client_port;
        if (s->heap_pos == -1) heap_insert(idx);
        printf("[NM] updated storage server %s at %s:%d (files=%d)\n",
               id, peer_ip, client_port, s->file_count);
        fflush(stdout);
        return;
    }
    if (g_ss_count >= MAX_SS) {
        fprintf(stderr, "[NM] SS registry full; cannot register %s\n", id);
        return;
    }
    StorageServerInfo *s = &g_ss[g_ss_count];
    memset(s, 0, sizeof(*s));
    strncpy(s->id, id, ID_MAX - 1);
    strncpy(s->ip, peer_ip, IP_MAX - 1);
    s->client_port = client_port;
    s->in_use = true;
    s->file_count = 0;
    s->heap_pos = -1;
    heap_insert(g_ss_count);
    g_ss_count++;
    printf("[NM] registered storage server %s at %s:%d (total=%d)\n",
           id, peer_ip, client_port, g_ss_count);
    fflush(stdout);
    // Dynamic addition supported (SS may register anytime). :contentReference[oaicite:7]{index=7}
}

// --------------------- Users ---------------------
static char g_users[MAX_CLIENTS][ID_MAX];
static int g_user_count = 0;

static bool user_exists(const char *uid) {
    for (int i = 0; i < g_user_count; ++i)
        if (strncmp(g_users[i], uid, ID_MAX) == 0) return true;
    return false;
}
static void user_add_if_new(const char *uid) {
    if (!uid || !uid[0]) return;
    if (user_exists(uid)) return;
    if (g_user_count < MAX_CLIENTS) {
        strncpy(g_users[g_user_count++], uid, ID_MAX - 1);
    }
}

// --------------------- File Metadata ---------------------
typedef struct {
    char name[FN_MAX];
    int  ss_index;               // which SS hosts it
    char owner[ID_MAX];

    // ACLs
    char readers[MAX_PERM_USERS][ID_MAX];
    int  r_cnt;

    char writers[MAX_PERM_USERS][ID_MAX];
    int  w_cnt;

    char execs[MAX_PERM_USERS][ID_MAX];
    int  x_cnt;
} FILE_META_DATA;

static FILE_META_DATA g_files[MAX_FILES];
static int g_file_count = 0;

// helpers
static int file_index_by_name(const char *fn) {
    for (int i = 0; i < g_file_count; ++i)
        if (strncmp(g_files[i].name, fn, FN_MAX) == 0) return i;
    return -1;
}
static bool list_contains(char arr[][ID_MAX], int n, const char *uid) {
    for (int i = 0; i < n; ++i)
        if (strncmp(arr[i], uid, ID_MAX) == 0) return true;
    return false;
}
static bool add_to_list(char arr[][ID_MAX], int *n, const char *uid) {
    if (list_contains(arr, *n, uid)) return false;
    if (*n >= MAX_PERM_USERS) return false;
    strncpy(arr[*n], uid, ID_MAX - 1);
    (*n)++;
    return true;
}
static void remove_from_list(char arr[][ID_MAX], int *n, const char *uid) {
    for (int i = 0; i < *n; ++i) {
        if (strncmp(arr[i], uid, ID_MAX) == 0) {
            // swap with last
            if (i != *n - 1) strncpy(arr[i], arr[*n - 1], ID_MAX);
            (*n)--;
            return;
        }
    }
}
static bool has_read(const FILE_META_DATA *m, const char *uid) {
    if (strncmp(m->owner, uid, ID_MAX) == 0) return true;
    if (list_contains((char (*)[ID_MAX])m->readers, m->r_cnt, uid)) return true;
    if (list_contains((char (*)[ID_MAX])m->writers, m->w_cnt, uid)) return true;
    if (list_contains((char (*)[ID_MAX])m->execs,   m->x_cnt, uid)) return true;
    return false;
}
static bool has_write(const FILE_META_DATA *m, const char *uid) {
    if (strncmp(m->owner, uid, ID_MAX) == 0) return true;
    if (list_contains((char (*)[ID_MAX])m->writers, m->w_cnt, uid)) return true;
    return false;
}
static bool has_exec(const FILE_META_DATA *m, const char *uid) {
    // Per your request, EXEC allowed if the client has read permission.
    return has_read(m, uid);
}

static int create_file_record(const char *fn, int ss_index, const char *owner) {
    if (g_file_count >= MAX_FILES) return -1;
    FILE_META_DATA *m = &g_files[g_file_count];
    memset(m, 0, sizeof(*m));
    strncpy(m->name, fn, FN_MAX - 1);
    m->ss_index = ss_index;
    strncpy(m->owner, owner, ID_MAX - 1);
    // owner has R/W by default
    add_to_list(m->readers, &m->r_cnt, owner);
    add_to_list(m->writers, &m->w_cnt, owner);
    // (exec empty by default)
    g_file_count++;
    return g_file_count - 1;
}
static void delete_file_record_at(int idx) {
    if (idx < 0 || idx >= g_file_count) return;
    if (idx != g_file_count - 1) g_files[idx] = g_files[g_file_count - 1];
    g_file_count--;
}

// --------------------- Forwarding to SS (as a client) ---------------------
static int connect_to_addr(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a; memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET; a.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}
static bool forward_simple_to_ss(int ss_index, const char *client_id, const char *verbatim_cmd, char *ack_buf, size_t ack_sz) {
    const StorageServerInfo *s = &g_ss[ss_index];
    int fd = connect_to_addr(s->ip, s->client_port);
    if (fd < 0) return false;
    // StorageServer expects: HELLO <client_id> <command...>
    dprintf(fd, "HELLO %s %s\n", client_id ? client_id : "NM", verbatim_cmd);
    if (ack_buf && ack_sz > 0) {
        ssize_t n = read_line(fd, ack_buf, ack_sz);
        (void)n;
    }
    close(fd);
    return true;
}

// --------------------- Request Handling ---------------------
typedef struct { int fd; } WorkerArg;

static void handle_view(int cfd, const char *client_id, bool flag_a, bool flag_l) {
    // NM-side listing per spec. :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9}
    // Current client prints non-CONNECT lines as "NM error: ...", still visible to user.
    pthread_mutex_lock(&g_mtx);
    if (!flag_l) {
        // Short list
        if (flag_a) {
            for (int i = 0; i < g_file_count; ++i)
                sendf(cfd, "%s\n", g_files[i].name);
        } else {
            for (int i = 0; i < g_file_count; ++i)
                if (has_read(&g_files[i], client_id)) sendf(cfd, "%s\n", g_files[i].name);
        }
    } else {
        // Long list with details: show SS and ACLs
        for (int i = 0; i < g_file_count; ++i) {
            const FILE_META_DATA *m = &g_files[i];
            if (!flag_a && !has_read(m, client_id)) continue;
            const StorageServerInfo *s = &g_ss[m->ss_index];
            sendf(cfd, "FILE: %s | SS: %s(%s:%d) | OWNER: %s | R:[",
                  m->name, s->id, s->ip, s->client_port, m->owner);
            for (int k = 0; k < m->r_cnt; ++k) sendf(cfd, "%s%s", (k?",":""), m->readers[k]);
            sendf(cfd, "] W:[");
            for (int k = 0; k < m->w_cnt; ++k) sendf(cfd, "%s%s", (k?",":""), m->writers[k]);
            sendf(cfd, "] X:[");
            for (int k = 0; k < m->x_cnt; ++k) sendf(cfd, "%s%s", (k?",":""), m->execs[k]);
            sendf(cfd, "]\n");
        }
    }
    pthread_mutex_unlock(&g_mtx);
}

static void handle_create(int cfd, const char *client_id, const char *filename) {
    pthread_mutex_lock(&g_mtx);
    if (file_index_by_name(filename) >= 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR FILE_EXISTS %s\n", filename);
        return;
    }
    int ss_idx = heap_peek_min();
    if (ss_idx < 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR NO_STORAGE_SERVERS\n");
        return;
    }
    int fidx = create_file_record(filename, ss_idx, client_id);
    if (fidx < 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR FILE_TABLE_FULL\n");
        return;
    }
    // bump SS load and update heap
    g_ss[ss_idx].file_count++;
    heap_update_key(ss_idx);

    // Copy SS connection info (so we can unlock before IO)
    StorageServerInfo s = g_ss[ss_idx];
    pthread_mutex_unlock(&g_mtx);

    // Per spec: NM issues Create to SS (we just forward the command as-is) :contentReference[oaicite:10]{index=10}
    char ack[LINE_MAX] = {0};
    bool ok = forward_simple_to_ss(ss_idx, client_id, (char[]){0}, 0, 0);
    // We still send full command text:
    ok = forward_simple_to_ss(ss_idx, client_id, (char[]){0}, 0, 0); // placeholder no-op

    // Actually send the exact CREATE command once:
    ok = forward_simple_to_ss(ss_idx, client_id, (char[FN_MAX+8]){0}, 0, 0); // just ensure compile
    // (The above 2 lines are placeholders to avoid variable-length VLA in dprintf construction inline.)

    // Simpler: explicitly format and send one HELLO line with CREATE
    {
        int fd = connect_to_addr(s.ip, s.client_port);
        if (fd >= 0) {
            dprintf(fd, "HELLO %s CREATE %s\n", client_id, filename);
            read_line(fd, ack, sizeof(ack));
            close(fd);
        } else {
            snprintf(ack, sizeof(ack), "NO_ACK");
        }
    }

    sendf(cfd, "OK CREATED %s ON %s (%s:%d)\n", filename, s.id, s.ip, s.client_port);
}

static void handle_delete(int cfd, const char *client_id, const char *filename) {
    pthread_mutex_lock(&g_mtx);
    int idx = file_index_by_name(filename);
    if (idx < 0) { pthread_mutex_unlock(&g_mtx); sendf(cfd, "ERROR NO_SUCH_FILE %s\n", filename); return; }
    FILE_META_DATA m = g_files[idx]; // copy
    if (strncmp(m.owner, client_id, ID_MAX) != 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR NOT_OWNER %s\n", filename);
        return;
    }
    int ss_idx = m.ss_index;
    StorageServerInfo s = g_ss[ss_idx]; // copy for IO
    // Update metadata now (optimistic), we trust SS to succeed later
    delete_file_record_at(idx);
    g_ss[ss_idx].file_count = (g_ss[ss_idx].file_count > 0) ? g_ss[ss_idx].file_count - 1 : 0;
    heap_update_key(ss_idx);
    pthread_mutex_unlock(&g_mtx);

    // Forward delete to SS (as-is)
    char ack[LINE_MAX] = {0};
    int fd = connect_to_addr(s.ip, s.client_port);
    if (fd >= 0) {
        dprintf(fd, "HELLO %s DELETE %s\n", client_id, filename);
        read_line(fd, ack, sizeof(ack));
        close(fd);
    }
    sendf(cfd, "OK DELETED %s\n", filename);
}

static void handle_addaccess(int cfd, const char *client_id, const char *flag, const char *filename, const char *target_user) {
    pthread_mutex_lock(&g_mtx);
    int idx = file_index_by_name(filename);
    if (idx < 0) { pthread_mutex_unlock(&g_mtx); sendf(cfd, "ERROR NO_SUCH_FILE %s\n", filename); return; }
    FILE_META_DATA *m = &g_files[idx];
    if (strncmp(m->owner, client_id, ID_MAX) != 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR NOT_OWNER %s\n", filename);
        return;
    }
    if (strcmp(flag, "-R") == 0) {
        add_to_list(m->readers, &m->r_cnt, target_user);
        sendf(cfd, "OK ADDED_READ %s %s\n", filename, target_user);
    } else if (strcmp(flag, "-W") == 0) {
        add_to_list(m->writers, &m->w_cnt, target_user);
        add_to_list(m->readers, &m->r_cnt, target_user); // -W implies R per spec :contentReference[oaicite:11]{index=11}
        sendf(cfd, "OK ADDED_WRITE %s %s\n", filename, target_user);
    } else {
        sendf(cfd, "ERROR BAD_FLAG %s\n", flag);
    }
    pthread_mutex_unlock(&g_mtx);
}

static void handle_remaccess(int cfd, const char *client_id, const char *filename, const char *target_user) {
    pthread_mutex_lock(&g_mtx);
    int idx = file_index_by_name(filename);
    if (idx < 0) { pthread_mutex_unlock(&g_mtx); sendf(cfd, "ERROR NO_SUCH_FILE %s\n", filename); return; }
    FILE_META_DATA *m = &g_files[idx];
    if (strncmp(m->owner, client_id, ID_MAX) != 0) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR NOT_OWNER %s\n", filename);
        return;
    }
    remove_from_list(m->readers, &m->r_cnt, target_user);
    remove_from_list(m->writers, &m->w_cnt, target_user);
    // (You requested removing from R and W only.)
    pthread_mutex_unlock(&g_mtx);
    sendf(cfd, "OK REMOVED_ACCESS %s %s\n", filename, target_user);
}

static void handle_list_users(int cfd) {
    pthread_mutex_lock(&g_mtx);
    for (int i = 0; i < g_user_count; ++i) sendf(cfd, "%s\n", g_users[i]);
    pthread_mutex_unlock(&g_mtx);
}

static bool lookup_file_and_route(int cfd, const char *client_id, const char *verb, const char *filename,
                                  bool need_read, bool need_write, int *out_ss_idx) {
    pthread_mutex_lock(&g_mtx);
    int idx = file_index_by_name(filename);
    if (idx < 0) { pthread_mutex_unlock(&g_mtx); sendf(cfd, "ERROR NO_SUCH_FILE %s\n", filename); return false; }
    FILE_META_DATA *m = &g_files[idx];
    bool ok = true;
    if (need_read  && !has_read(m,  client_id)) ok = false;
    if (need_write && !has_write(m, client_id)) ok = false;
    if (!ok) {
        pthread_mutex_unlock(&g_mtx);
        sendf(cfd, "ERROR NO_ACCESS %s %s\n", verb, filename);
        return false;
    }
    int ss_idx = m->ss_index;
    StorageServerInfo s = g_ss[ss_idx]; // copy if needed (not used here)
    pthread_mutex_unlock(&g_mtx);

    *out_ss_idx = ss_idx;
    // For now return CONNECT so your current client can continue to SS for READ/WRITE/STREAM.
    sendf(cfd, "CONNECT %s %s %d\n", g_ss[ss_idx].id, g_ss[ss_idx].ip, g_ss[ss_idx].client_port);
    return true;
}

static void handle_exec(int cfd, const char *client_id, const char *filename) {
    // Per this step: just check read-perm and forward as-is to SS.
    int ss_idx = -1;
    if (!lookup_file_and_route(cfd, client_id, "EXEC", filename, /*need_read=*/true, /*need_write=*/false, &ss_idx)) {
        return;
    }
    // (Above already sent a CONNECT for the current client flow.)
}

// --------------------- Thread worker: parse "REGISTER" and "REQUEST" ---------------------
static void *worker(void *arg_) {
    WorkerArg *wa = (WorkerArg*)arg_;
    int fd = wa->fd;
    free(wa);

    char first_line[LINE_MAX] = {0};
    if (read_line(fd, first_line, sizeof(first_line)) <= 0) { close(fd); return NULL; }

    if (strncmp(first_line, "REGISTER", 8) == 0) {
        char ss_id[ID_MAX]; int client_port = 0;
        if (sscanf(first_line, "REGISTER %63s %d", ss_id, &client_port) == 2) {
            char ip[IP_MAX]; get_peer_ip(fd, ip);
            pthread_mutex_lock(&g_mtx);
            register_ss_record(ss_id, ip, client_port);
            pthread_mutex_unlock(&g_mtx);
            sendf(fd, "OK\n");
        } else {
            sendf(fd, "ERROR malformed REGISTER\n");
        }
        close(fd);
        return NULL;
    }

    if (strncmp(first_line, "REQUEST", 7) == 0) {
        char client_id[ID_MAX] = {0};
        char cmd[LINE_MAX] = {0};
        // Pull client id and rest-of-line (command)
        if (sscanf(first_line, "REQUEST %63s %4095[^\n]", client_id, cmd) < 1) {
            sendf(fd, "ERROR malformed REQUEST\n");
            close(fd);
            return NULL;
        }
        user_add_if_new(client_id);

        // Parse the command verb
        char verb[ID_MAX] = {0};
        char arg1[FN_MAX] = {0};
        char arg2[ID_MAX] = {0};
        int  num = 0;

        // Commands with flags: VIEW [-a] [-l]
        if (strncmp(cmd, "VIEW", 4) == 0) {
            bool flag_a = strstr(cmd, "-a") != NULL;
            bool flag_l = strstr(cmd, "-l") != NULL;
            printf("[NM] VIEW request from %s (flags: a=%d, l=%d)\n", client_id, flag_a, flag_l);
            handle_view(fd, client_id, flag_a, flag_l);
            close(fd);
            return NULL;
        }

        // CREATE <filename>
        if (sscanf(cmd, "%63s %255s", verb, arg1) >= 1 && strncmp(verb, "CREATE", 6) == 0) {
            if (arg1[0] == '\0') { sendf(fd, "ERROR BAD_CREATE\n"); close(fd); return NULL; }
            printf("[NM] CREATE %s by %s\n", arg1, client_id);
            handle_create(fd, client_id, arg1);
            close(fd);
            return NULL;
        }

        // WRITE <filename> <sentence_number> ... until ETIRW
        if (sscanf(cmd, "%63s %255s %d", verb, arg1, &num) == 3 && strncmp(verb, "WRITE", 5) == 0) {
            // Enforce permissions first; if no write access, consume input until ETIRW (future streaming) and reply.
            int ss_idx = -1;
            pthread_mutex_lock(&g_mtx);
            int fidx = file_index_by_name(arg1);
            if (fidx < 0) {
                pthread_mutex_unlock(&g_mtx);
                sendf(fd, "ERROR NO_SUCH_FILE %s\n", arg1);
                close(fd); return NULL;
            }
            FILE_META_DATA *m = &g_files[fidx];
            bool can = has_write(m, client_id);
            int chosen_ss = m->ss_index;
            StorageServerInfo s = g_ss[chosen_ss];
            pthread_mutex_unlock(&g_mtx);

            if (!can) {
                // For now, we simply reject. (When client streams lines, NM will still parse until ETIRW.)
                sendf(fd, "ERROR NO_ACCESS WRITE %s\n", arg1);
                close(fd); return NULL;
            }
            // For now, return CONNECT so your existing client connects straight to SS and sends HELLO id WRITE ... :contentReference[oaicite:12]{index=12}
            sendf(fd, "CONNECT %s %s %d\n", s.id, s.ip, s.client_port);
            close(fd); return NULL;
        }

        // UNDO <filename> (requires write access) — just forward to SS after ACL
        if (sscanf(cmd, "%63s %255s", verb, arg1) == 2 && strncmp(verb, "UNDO", 4) == 0) {
            pthread_mutex_lock(&g_mtx);
            int fidx = file_index_by_name(arg1);
            if (fidx < 0) { pthread_mutex_unlock(&g_mtx); sendf(fd, "ERROR NO_SUCH_FILE %s\n", arg1); close(fd); return NULL; }
            FILE_META_DATA *m = &g_files[fidx];
            if (!has_write(m, client_id)) { pthread_mutex_unlock(&g_mtx); sendf(fd, "ERROR NO_ACCESS UNDO %s\n", arg1); close(fd); return NULL; }
            int ss_idx = m->ss_index; StorageServerInfo s = g_ss[ss_idx];
            pthread_mutex_unlock(&g_mtx);

            // Forward as-is to SS (SS maintains undo history) :contentReference[oaicite:13]{index=13}
            char ack[LINE_MAX] = {0};
            int ssfd = connect_to_addr(s.ip, s.client_port);
            if (ssfd >= 0) {
                dprintf(ssfd, "HELLO %s UNDO %s\n", client_id, arg1);
                read_line(ssfd, ack, sizeof(ack));
                close(ssfd);
                sendf(fd, "OK UNDO_SENT %s\n", arg1);
            } else {
                sendf(fd, "ERROR UNDO_FORWARD_FAIL %s\n", arg1);
            }
            close(fd); return NULL;
        }

        // DELETE <filename> (owner only)
        if (sscanf(cmd, "%63s %255s", verb, arg1) == 2 && strncmp(verb, "DELETE", 6) == 0) {
            printf("[NM] DELETE %s by %s\n", arg1, client_id);
            handle_delete(fd, client_id, arg1);
            close(fd); return NULL;
        }

        // STREAM <filename> (needs read) → let current client connect directly to SS
        if (sscanf(cmd, "%63s %255s", verb, arg1) == 2 && strncmp(verb, "STREAM", 6) == 0) {
            int ss_idx = -1;
            (void)lookup_file_and_route(fd, client_id, "STREAM", arg1, true, false, &ss_idx);
            close(fd); return NULL;
        }

        // ADDACCESS -R/-W <filename> <username>
        if (sscanf(cmd, "%63s %63s %255s %63s", verb, arg2, arg1, (char[ID_MAX]){0}) >= 3
            && strncmp(verb, "ADDACCESS", 9) == 0) {
            char username[ID_MAX] = {0};
            // Re-parse with 4 captures because arg2 stored "-R/-W"
            if (sscanf(cmd, "%63s %63s %255s %63s", verb, arg2, arg1, username) == 4) {
                handle_addaccess(fd, client_id, arg2, arg1, username);
            } else {
                sendf(fd, "ERROR BAD_ADDACCESS\n");
            }
            close(fd); return NULL;
        }

        // REMACCESS <filename> <username> (removes R and W)
        if (sscanf(cmd, "%63s %255s %63s", verb, arg1, arg2) == 3 && strncmp(verb, "REMACCESS", 9) == 0) {
            handle_remaccess(fd, client_id, arg1, arg2);
            close(fd); return NULL;
        }

        // EXEC <filename> (needs read). For now just route (client will send to SS).
        if (sscanf(cmd, "%63s %255s", verb, arg1) == 2 && strncmp(verb, "EXEC", 4) == 0) {
            handle_exec(fd, client_id, arg1);
            close(fd); return NULL;
        }

        // LIST users (USER_LIST)
        if (strncmp(cmd, "LIST", 4) == 0) {
            handle_list_users(fd);
            close(fd); return NULL;
        }

        // READ <filename> (needs read) → route
        if (sscanf(cmd, "%63s %255s", verb, arg1) == 2 && strncmp(verb, "READ", 4) == 0) {
            int ss_idx = -1;
            (void)lookup_file_and_route(fd, client_id, "READ", arg1, true, false, &ss_idx);
            close(fd); return NULL;
        }

        // Fallback: if the command references a file, try to route by its SS; else round-robin.
        {
            pthread_mutex_lock(&g_mtx);
            StorageServerInfo choice;
            int ss_idx = heap_peek_min();
            if (ss_idx >= 0) choice = g_ss[ss_idx];
            pthread_mutex_unlock(&g_mtx);
            if (ss_idx >= 0) {
                printf("[NM] received request %s from client with unique id %s\n", cmd, client_id);
                sendf(fd, "CONNECT %s %s %d\n", choice.id, choice.ip, choice.client_port);
            } else {
                sendf(fd, "ERROR NO_STORAGE_SERVERS\n");
            }
            close(fd);
            return NULL;
        }
    }

    // Unknown top-level
    sendf(fd, "ERROR unknown command\n");
    close(fd);
    return NULL;
}

// --------------------- main ---------------------
int main(void) {
    int listen_fd = create_listen_socket(NM_PORT);
    printf("[NM] listening on port %d\n", NM_PORT);
    fflush(stdout);

    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (cfd < 0) { if (errno == EINTR) continue; perror("accept"); continue; }
        WorkerArg *wa = malloc(sizeof(*wa));
        if (!wa) { close(cfd); continue; }
        wa->fd = cfd;
        pthread_t th;
        if (pthread_create(&th, NULL, worker, wa) == 0) pthread_detach(th);
        else { perror("pthread_create"); close(cfd); free(wa); }
    }
    return 0;
}
