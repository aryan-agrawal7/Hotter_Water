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
#include <time.h>
#include <signal.h>
#include <dirent.h>
#include <stdarg.h>

#define ID_MAX  64
#define FNAME_MAX 256

static char g_nm_ip[64] = "127.0.0.1";
static int g_nm_port = 5000;

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

static char g_storage_dir[PATH_MAX];

static int storage_dir_for(const char *name, char *out, size_t out_sz);
static int ensure_file_container(const char *name);
static int storage_path_for(const char *name, char *out, size_t out_sz);
static int undo_path_for(const char *name, char *out, size_t out_sz);
static int swap_path_for(const char *name, char *out, size_t out_sz);
static int connect_to_nm(const char *ip, int port);
static ssize_t read_line(int fd, char *buf, size_t maxlen);

static void send_existing_files_to_nm(const char *ss_id) {
    DIR *d = opendir(g_storage_dir);
    if (!d) return;

    char file_list[4096] = {0};
    struct dirent *dir;
    bool first = true;
    int count = 0;

    while ((dir = readdir(d)) != NULL) {
        if (strcmp(dir->d_name, ".") == 0 || strcmp(dir->d_name, "..") == 0) continue;
        
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", g_storage_dir, dir->d_name);
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            if (!first) {
                strncat(file_list, ", ", sizeof(file_list) - strlen(file_list) - 1);
            }
            strncat(file_list, dir->d_name, sizeof(file_list) - strlen(file_list) - 1);
            first = false;
            count++;
        }
    }
    closedir(d);

    if (count > 0) {
        int nmfd = connect_to_nm(g_nm_ip, g_nm_port);
        if (nmfd >= 0) {
            dprintf(nmfd, "SS_LOG_FILES %s %s\n", ss_id, file_list);
            char resp[LINE_MAX];
            read_line(nmfd, resp, sizeof(resp)); // Wait for ack
            close(nmfd);
            log_message("INFO", "[SS %s] Informed NM about %d existing files: %s", ss_id, count, file_list);
        }
    } else {
        log_message("INFO", "[SS %s] No existing files found to report.", ss_id);
    }
}

static int create_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { 
        log_message("ERROR", "Socket Error in function ss.create_listen_socket(): %s", strerror(errno)); 
        exit(1); 
    }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_message("ERROR", "bind error in function ss.create_listen_socket(): %s", strerror(errno)); 
        exit(1);
    }
    if (listen(fd, 128) < 0) { 
        log_message("ERROR", "listen error in function ss.create_listen_socket(): %s", strerror(errno)); 
        exit(1); 
    }
    return fd;
}

static int connect_to_nm(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { 
        log_message("ERROR", "socket error in function ss.connect_to_nm(): %s", strerror(errno)); 
        exit(1); 
    }
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        log_message("ERROR", "Named Server IP is invalid"); 
        exit(1);
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_message("ERROR", "connect NM error in function ss.connect_to_nm(): %s", strerror(errno)); 
        exit(1);
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
    if (ensure_file_container(filename) < 0) {
        return -1;
    }
    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        return -1;
    }
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd < 0) {
        return -1;
    }
    close(fd);

    char undo_path[PATH_MAX];
    if (undo_path_for(filename, undo_path, sizeof(undo_path)) == 0) {
        unlink(undo_path);
    }
    return 0;
}

// metadata directory for each file now reuses the file container directory
static int meta_dir_for(const char *filename, char *out, size_t out_sz) {
    char dir[PATH_MAX];
    if (storage_dir_for(filename, dir, sizeof(dir)) < 0) {
        return -1;
    }

    struct stat st;
    if (stat(dir, &st) != 0) {
        if (errno != ENOENT) {
            return -1;
        }
        if (mkdir(dir, 0777) < 0) {
            return -1;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    if (out) {
        int n = snprintf(out, out_sz, "%s", dir);
        if (n < 0 || (size_t)n >= out_sz) {
            errno = ENAMETOOLONG;
            return -1;
        }
    }
    return 0;
}

static void compute_word_line_counts(const char *path, long long *word_count, long long *line_count) {
    if (word_count) *word_count = 0;
    if (line_count) *line_count = 0;

    if (!word_count && !line_count) return;

    FILE *f = fopen(path, "r");
    if (!f) return;

    long long words = 0;
    long long lines = 0;
    bool in_word = false;
    bool saw_char = false;
    int last_char = -1;
    int c;
    while ((c = fgetc(f)) != EOF) {
        saw_char = true;
        if (c == '\n') lines++;
        if (isspace((unsigned char)c)) {
            in_word = false;
        } else {
            if (!in_word) {
                words++;
                in_word = true;
            }
        }
        last_char = c;
    }
    fclose(f);

    if (saw_char && last_char != '\n') lines++;

    if (word_count) *word_count = words;
    if (line_count) *line_count = lines;
}

static int write_info_txt(const char *filename, const char *owner, const char *access_str, const char *created_ts, const char *lastmod_ts, const char *last_access_ts, const char *last_access_by) {
    char dir[PATH_MAX];
    if (meta_dir_for(filename, dir, sizeof(dir)) < 0) return -1;
    char info_path[PATH_MAX];
    if (snprintf(info_path, sizeof(info_path), "%s/info.txt", dir) < 0) return -1;

    // determine size
    char data_path[PATH_MAX];
    if (storage_path_for(filename, data_path, sizeof(data_path)) < 0) return -1;
    struct stat st;
    long long size = 0;
    if (stat(data_path, &st) == 0 && S_ISREG(st.st_mode)) size = st.st_size;

    long long word_count = 0;
    long long line_count = 0;
    compute_word_line_counts(data_path, &word_count, &line_count);

    FILE *f = fopen(info_path, "w");
    if (!f) return -1;
    fprintf(f, "File: %s\n", filename);
    fprintf(f, "Owner: %s\n", owner ? owner : "");
    fprintf(f, "Created: %s\n", created_ts ? created_ts : "");
    fprintf(f, "Last Modified: %s\n", lastmod_ts ? lastmod_ts : "");
    fprintf(f, "Size: %lld bytes\n", size);
    fprintf(f, "Word Count: %lld\n", word_count);
    fprintf(f, "Line Count: %lld\n", line_count);
    fprintf(f, "Access: %s\n", access_str ? access_str : "");
    fprintf(f, "Last Accessed: %s by %s\n", last_access_ts ? last_access_ts : "", last_access_by? last_access_by : "");
    fclose(f);
    return 0;
}

static void format_time_now(char *out, size_t out_sz) {
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(out, out_sz, "%Y-%m-%d %H:%M:%S", &tm);
}

// load simple fields from info.txt if present; missing fields left empty
static void load_info_fields(const char *filename,
                            char *owner, size_t owner_sz,
                            char *access_str, size_t access_sz,
                            char *created_ts, size_t created_sz,
                            char *lastmod_ts, size_t lastmod_sz,
                            char *last_access_ts, size_t last_access_sz,
                            char *last_access_user, size_t last_access_user_sz) {
    owner[0]='\0'; access_str[0]='\0'; created_ts[0]='\0'; lastmod_ts[0]='\0';
    last_access_ts[0]='\0'; last_access_user[0]='\0';
    char dir[PATH_MAX];
    if (meta_dir_for(filename, dir, sizeof(dir)) < 0) return;
    char info_path[PATH_MAX];
    if (snprintf(info_path, sizeof(info_path), "%s/info.txt", dir) < 0) return;
    FILE *f = fopen(info_path, "r");
    if (!f) return;
    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "Owner:", 6) == 0) {
            char *p = line + 6; while (*p && isspace((unsigned char)*p)) p++; trim_spaces(p); p[strcspn(p, "\r\n")] = '\0'; strncpy(owner, p, owner_sz-1); owner[owner_sz-1]='\0';
        } else if (strncmp(line, "Access:", 7) == 0) {
            char *p = line + 7; while (*p && isspace((unsigned char)*p)) p++; trim_spaces(p); p[strcspn(p, "\r\n")] = '\0'; strncpy(access_str, p, access_sz-1); access_str[access_sz-1]='\0';
        } else if (strncmp(line, "Created:", 8) == 0) {
            char *p = line + 8; while (*p && isspace((unsigned char)*p)) p++; trim_spaces(p); p[strcspn(p, "\r\n")] = '\0'; strncpy(created_ts, p, created_sz-1); created_ts[created_sz-1]='\0';
        } else if (strncmp(line, "Last Modified:", 14) == 0) {
            char *p = line + 14; while (*p && isspace((unsigned char)*p)) p++; trim_spaces(p); p[strcspn(p, "\r\n")] = '\0'; strncpy(lastmod_ts, p, lastmod_sz-1); lastmod_ts[lastmod_sz-1]='\0';
        } else if (strncmp(line, "Last Accessed:", 14) == 0) {
            char *p = line + 14; while (*p && isspace((unsigned char)*p)) p++;
            char *by = strstr(p, " by ");
            if (by) {
                *by = '\0'; by += 4;
                p[strcspn(p, "\r\n")] = '\0';
                trim_spaces(p);
                strncpy(last_access_ts, p, last_access_sz-1); last_access_ts[last_access_sz-1]='\0';
                trim_spaces(by);
                by[strcspn(by, "\r\n")] = '\0';
                strncpy(last_access_user, by, last_access_user_sz-1); last_access_user[last_access_user_sz-1]='\0';
            } else {
                trim_spaces(p);
                p[strcspn(p, "\r\n")] = '\0';
                strncpy(last_access_ts, p, last_access_sz-1); last_access_ts[last_access_sz-1]='\0';
            }
        }
    }
    fclose(f);
}

static int delete_storage_file(const char *filename) {
    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        return -1;
    }
    if (unlink(path) < 0) {
        return -1;
    }

    char undo_path[PATH_MAX];
    if (undo_path_for(filename, undo_path, sizeof(undo_path)) == 0) {
        unlink(undo_path);
    }
    char swap_path[PATH_MAX];
    if (swap_path_for(filename, swap_path, sizeof(swap_path)) == 0) {
        unlink(swap_path);
    }

    char dir[PATH_MAX];
    if (storage_dir_for(filename, dir, sizeof(dir)) == 0) {
        char info_path[PATH_MAX];
        int info_len = snprintf(info_path, sizeof(info_path), "%s/info.txt", dir);
        if (info_len >= 0 && (size_t)info_len < sizeof(info_path)) {
            unlink(info_path);
        }
        rmdir(dir);
    }
    return 0;
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

static int write_all_fd(int fd, const char *buf, size_t len);

static void send_stop_packet(int fd, const char *verb, const char *fname) {
    const char *name = (fname && *fname) ? fname : "-";
    dprintf(fd, "STOP %s %s\n", verb, name);
}

typedef struct WordNode {
    char *data;
    struct WordNode *next;
    struct WordNode *prev;
} WordNode;

typedef struct SentenceNode {
    WordNode *words_head;
    WordNode *words_tail;
    struct SentenceNode *next;
    struct SentenceNode *prev;
    bool locked;
    char locked_by[ID_MAX];
} SentenceNode;

typedef struct {
    char filename[ID_MAX];
    SentenceNode *head;
    SentenceNode *tail;
    size_t count;
    int ref_count;
} Document;

typedef struct {
    bool in_use;
    char client_id[ID_MAX];
    char filename[ID_MAX];
    Document *doc;
    SentenceNode *target;
    SentenceNode *draft_head;
    SentenceNode *draft_tail;
    mode_t original_mode;
} WriteSession;

static Document *get_document(const char *filename);
static void invalidate_document(const char *filename);

static WordNode *word_new(const char *text) {
    WordNode *w = malloc(sizeof(WordNode));
    if (!w) return NULL;
    w->data = strdup(text);
    w->next = w->prev = NULL;
    return w;
}

static void word_free(WordNode *w) {
    if (!w) return;
    free(w->data);
    free(w);
}

static SentenceNode *sentence_new(void) {
    SentenceNode *s = malloc(sizeof(SentenceNode));
    if (!s) return NULL;
    s->words_head = s->words_tail = NULL;
    s->next = s->prev = NULL;
    s->locked = false;
    s->locked_by[0] = '\0';
    return s;
}

static void sentence_append_word(SentenceNode *s, const char *text) {
    WordNode *w = word_new(text);
    if (!w) return;
    if (!s->words_head) {
        s->words_head = s->words_tail = w;
    } else {
        s->words_tail->next = w;
        w->prev = s->words_tail;
        s->words_tail = w;
    }
}

static void sentence_free(SentenceNode *s) {
    if (!s) return;
    WordNode *cur = s->words_head;
    while (cur) {
        WordNode *next = cur->next;
        word_free(cur);
        cur = next;
    }
    free(s);
}

static void document_init(Document *doc, const char *filename) {
    strncpy(doc->filename, filename, ID_MAX-1);
    doc->filename[ID_MAX-1] = '\0';
    doc->head = doc->tail = NULL;
    doc->count = 0;
    doc->ref_count = 0;
}

static void document_append_sentence(Document *doc, SentenceNode *s) {
    if (!doc->head) {
        doc->head = doc->tail = s;
    } else {
        doc->tail->next = s;
        s->prev = doc->tail;
        doc->tail = s;
    }
    doc->count++;
}

static void document_free_content(Document *doc) {
    SentenceNode *cur = doc->head;
    while (cur) {
        SentenceNode *next = cur->next;
        sentence_free(cur);
        cur = next;
    }
    doc->head = doc->tail = NULL;
    doc->count = 0;
}

static SentenceNode *sentence_copy(SentenceNode *src) {
    SentenceNode *dst = sentence_new();
    WordNode *cur = src->words_head;
    while (cur) {
        sentence_append_word(dst, cur->data);
        cur = cur->next;
    }
    return dst;
}

static bool is_sentence_delimiter(char c) {
    return c == '.' || c == '!' || c == '?' || c == '\n';
}


static int document_load_from_file(const char *filename, Document *doc, mode_t *mode_out) {
    // Initialize with the LOGICAL filename (not the on-disk path) so in-memory cache matches future lookups.
    document_init(doc, filename);

    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) return -1;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return -1; }
    if (mode_out) *mode_out = st.st_mode;

    size_t size = (size_t)st.st_size;
    char *buffer = malloc(size + 1);
    if (!buffer) { close(fd); return -1; }

    size_t off = 0;
    while (off < size) {
        ssize_t r = read(fd, buffer + off, size - off);
        if (r < 0) { if (errno == EINTR) continue; free(buffer); close(fd); return -1; }
        if (r == 0) break;
        off += (size_t)r;
    }
    size = off;
    buffer[off] = '\0';
    close(fd);

    SentenceNode *current = NULL;
    size_t word_start = (size_t)-1;
    bool skip_newline = false;

    for (size_t i = 0; i <= size; ++i) {
        char c = (i < size) ? buffer[i] : '\0';
        bool at_end = (i == size);
        
        bool is_punct = (c == '.' || c == '!' || c == '?');
        bool is_newline = (c == '\n');
        bool is_space = isspace((unsigned char)c) && !is_newline;

        if (is_punct) {
            if (word_start != (size_t)-1) {
                size_t len = i - word_start;
                char *w = malloc(len + 2);
                memcpy(w, buffer + word_start, len);
                w[len] = c; w[len+1] = '\0';
                if (!current) { current = sentence_new(); document_append_sentence(doc, current); }
                sentence_append_word(current, w);
                free(w);
                word_start = (size_t)-1;
            } else {
                char w[2] = {c, '\0'};
                if (!current) { current = sentence_new(); document_append_sentence(doc, current); }
                sentence_append_word(current, w);
            }
            current = NULL; // End sentence
            skip_newline = true;
        } else if (is_newline) {
            if (skip_newline) {
                skip_newline = false;
            } else {
                if (word_start != (size_t)-1) {
                    size_t len = i - word_start;
                    char *w = strndup(buffer + word_start, len);
                    if (!current) { current = sentence_new(); document_append_sentence(doc, current); }
                    sentence_append_word(current, w);
                    free(w);
                    word_start = (size_t)-1;
                } else {
                    if (!current) {
                        current = sentence_new();
                        document_append_sentence(doc, current);
                    }
                }
                current = NULL; // End sentence
                skip_newline = false;
            }
        } else if (is_space || at_end) {
            if (word_start != (size_t)-1) {
                size_t len = i - word_start;
                char *w = strndup(buffer + word_start, len);
                if (!current) { current = sentence_new(); document_append_sentence(doc, current); }
                sentence_append_word(current, w);
                free(w);
                word_start = (size_t)-1;
            }
        } else {
            if (word_start == (size_t)-1) word_start = i;
            skip_newline = false;
        }
    }
    free(buffer);
    return 0;
}


static char *sentence_join_words(const SentenceNode *s) {
    if (!s || !s->words_head) return strdup("");
    size_t len = 0;
    WordNode *cur = s->words_head;
    while (cur) {
        len += strlen(cur->data);
        if (cur->next) len++;
        cur = cur->next;
    }
    char *buf = malloc(len + 1);
    if (!buf) return NULL;
    buf[0] = '\0';
    cur = s->words_head;
    while (cur) {
        strcat(buf, cur->data);
        if (cur->next) strcat(buf, " ");
        cur = cur->next;
    }
    return buf;
}
static int write_all_fd(int fd, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t w = write(fd, buf + off, len - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) {
            errno = EIO;
            return -1;
        }
        off += (size_t)w;
    }
    return 0;
}

static int write_document_to_path(const Document *doc, const char *path, mode_t mode) {
    char tmp_template[PATH_MAX];
    int written = snprintf(tmp_template, sizeof(tmp_template), "%s.tmpXXXXXX", path);
    if (written < 0 || (size_t)written >= sizeof(tmp_template)) {
        errno = ENAMETOOLONG;
        return -1;
    }

    int tmp_fd = mkstemp(tmp_template);
    if (tmp_fd < 0) return -1;

    if (fchmod(tmp_fd, mode) < 0) {
        int saved = errno;
        close(tmp_fd);
        unlink(tmp_template);
        errno = saved;
        return -1;
    }

    SentenceNode *s = doc->head;
    while (s) {
        char *joined = sentence_join_words(s);
        if (!joined) {
            int saved = errno;
            close(tmp_fd);
            unlink(tmp_template);
            errno = saved;
            return -1;
        }
        size_t join_len = strlen(joined);
        if (join_len > 0) {
            if (write_all_fd(tmp_fd, joined, join_len) < 0) {
                int saved = errno;
                free(joined);
                close(tmp_fd);
                unlink(tmp_template);
                errno = saved;
                return -1;
            }
        }
        
        // Only write newline if the sentence doesn't already end with one
        if (join_len == 0 || joined[join_len - 1] != '\n') {
            if (write_all_fd(tmp_fd, "\n", 1) < 0) {
                int saved = errno;
                free(joined);
                close(tmp_fd);
                unlink(tmp_template);
                errno = saved;
                return -1;
            }
        }
        free(joined);
        s = s->next;
    }

    if (fsync(tmp_fd) < 0) {
        int saved = errno;
        close(tmp_fd);
        unlink(tmp_template);
        errno = saved;
        return -1;
    }
    if (close(tmp_fd) < 0) {
        int saved = errno;
        unlink(tmp_template);
        errno = saved;
        return -1;
    }
    if (rename(tmp_template, path) < 0) {
        int saved = errno;
        unlink(tmp_template);
        errno = saved;
        return -1;
    }
    return 0;
}

static int storage_dir_for(const char *name, char *out, size_t out_sz) {
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

static int ensure_file_container(const char *name) {
    char dir[PATH_MAX];
    if (storage_dir_for(name, dir, sizeof(dir)) < 0) {
        return -1;
    }
    struct stat st;
    if (stat(dir, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            errno = ENOTDIR;
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) {
        return -1;
    }
    if (mkdir(dir, 0777) < 0) {
        return -1;
    }
    return 0;
}

static int storage_path_for(const char *name, char *out, size_t out_sz) {
    char dir[PATH_MAX];
    if (storage_dir_for(name, dir, sizeof(dir)) < 0) {
        return -1;
    }
    int written = snprintf(out, out_sz, "%s/%s", dir, name);
    if (written < 0 || (size_t)written >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int undo_path_for(const char *name, char *out, size_t out_sz) {
    char dir[PATH_MAX];
    if (storage_dir_for(name, dir, sizeof(dir)) < 0) {
        return -1;
    }
    int written = snprintf(out, out_sz, "%s/.undo", dir);
    if (written < 0 || (size_t)written >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int swap_path_for(const char *name, char *out, size_t out_sz) {
    char dir[PATH_MAX];
    if (storage_dir_for(name, dir, sizeof(dir)) < 0) {
        return -1;
    }
    int written = snprintf(out, out_sz, "%s/.swap", dir);
    if (written < 0 || (size_t)written >= out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int ensure_checkpoints_dir(void) {
    char ckpt_dir[PATH_MAX];
    int written = snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/checkpoints", g_storage_dir);
    if (written < 0 || (size_t)written >= sizeof(ckpt_dir)) return -1;
    
    struct stat st;
    if (stat(ckpt_dir, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            log_message("ERROR", "checkpoints path exists but is not a directory");
            return -1;
        }
        return 0;
    }
    if (errno != ENOENT) return -1;
    if (mkdir(ckpt_dir, 0777) < 0) {
        log_message("ERROR", "failed to create checkpoints directory: %s", strerror(errno));
        return -1;
    }
    return 0;
}

static int checkpoint_path_for(const char *filename, const char *tag, char *out, size_t out_sz) {
    if (ensure_checkpoints_dir() < 0) return -1;
    int written = snprintf(out, out_sz, "%s/checkpoints/%s_%s", g_storage_dir, filename, tag);
    if (written < 0 || (size_t)written >= out_sz) {
        log_message("ERROR", "checkpoint path truncation");
        return -1;
    }
    return 0;
}

static int copy_file_to_path(const char *src_path, const char *dst_path) {
    int src = open(src_path, O_RDONLY);
    if (src < 0) {
        return -1;
    }

    struct stat st;
    if (fstat(src, &st) < 0) {
        int saved = errno;
        close(src);
        errno = saved;
        return -1;
    }

    char tmp_template[PATH_MAX];
    int written = snprintf(tmp_template, sizeof(tmp_template), "%s.tmpXXXXXX", dst_path);
    if (written < 0 || (size_t)written >= sizeof(tmp_template)) {
        close(src);
        errno = ENAMETOOLONG;
        return -1;
    }

    int dst = mkstemp(tmp_template);
    if (dst < 0) {
        int saved = errno;
        close(src);
        errno = saved;
        return -1;
    }

    if (fchmod(dst, st.st_mode) < 0) {
        int saved = errno;
        close(dst);
        unlink(tmp_template);
        close(src);
        errno = saved;
        return -1;
    }

    char buffer[4096];
    for (;;) {
        ssize_t r = read(src, buffer, sizeof(buffer));
        if (r < 0) {
            if (errno == EINTR) continue;
            int saved = errno;
            close(dst);
            unlink(tmp_template);
            close(src);
            errno = saved;
            return -1;
        }
        if (r == 0) break;
        if (write_all_fd(dst, buffer, (size_t)r) < 0) {
            int saved = errno;
            close(dst);
            unlink(tmp_template);
            close(src);
            errno = saved;
            return -1;
        }
    }

    if (fsync(dst) < 0) {
        int saved = errno;
        close(dst);
        unlink(tmp_template);
        close(src);
        errno = saved;
        return -1;
    }

    if (close(dst) < 0) {
        int saved = errno;
        unlink(tmp_template);
        close(src);
        errno = saved;
        return -1;
    }
    close(src);

    if (unlink(dst_path) < 0 && errno != ENOENT) {
        int saved = errno;
        unlink(tmp_template);
        errno = saved;
        return -1;
    }
    if (rename(tmp_template, dst_path) < 0) {
        int saved = errno;
        unlink(tmp_template);
        errno = saved;
        return -1;
    }
    return 0;
}

static WriteSession g_write_sessions[32];

static WriteSession *session_find_by_client(const char *client_id) {
    for (size_t i = 0; i < 32; ++i) {
        if (g_write_sessions[i].in_use && strcmp(g_write_sessions[i].client_id, client_id) == 0) {
            return &g_write_sessions[i];
        }
    }
    return NULL;
}

static WriteSession *session_allocate(void) {
    for (size_t i = 0; i < 32; ++i) {
        if (!g_write_sessions[i].in_use) {
            g_write_sessions[i].in_use = true;
            g_write_sessions[i].doc = NULL;
            g_write_sessions[i].target = NULL;
            g_write_sessions[i].draft_head = NULL;
            g_write_sessions[i].draft_tail = NULL;
            return &g_write_sessions[i];
        }
    }
    return NULL;
}

static void session_release(WriteSession *session) {
    if (!session) return;
    SentenceNode *cur = session->draft_head;
    while (cur) {
        SentenceNode *next = cur->next;
        sentence_free(cur);
        cur = next;
    }
    if (session->target) {
        session->target->locked = false;
        session->target->locked_by[0] = '\0';
    }
    if (session->doc) {
        session->doc->ref_count--;
    }
    session->in_use = false;
}

static bool file_has_active_session(const char *filename) {
    for (size_t i = 0; i < 32; ++i) {
        if (!g_write_sessions[i].in_use) continue;
        if (strcmp(g_write_sessions[i].filename, filename) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_number_string(const char *s) {
    if (!s || *s == '\0') return false;
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        if (!isdigit(*p)) return false;
    }
    return true;
}

static void send_sentence_snapshot(int cfd, const SentenceNode *sentence) {
    char *joined = sentence_join_words(sentence);
    if (!joined) {
        dprintf(cfd, "SENTENCE ERROR\n");
        return;
    }
    if (joined[0] == '\0') {
        dprintf(cfd, "SENTENCE (empty)\n");
    } else {
        dprintf(cfd, "SENTENCE %s\n", joined);
    }
    free(joined);
}

// Check if a sentence ends with a delimiter
static bool sentence_ends_with_delimiter(const SentenceNode *s) {
    if (!s || !s->words_tail) return false;
    const char *last_word = s->words_tail->data;
    if (!last_word || *last_word == '\0') return false;
    size_t len = strlen(last_word);
    char last_char = last_word[len - 1];
    return is_sentence_delimiter(last_char);
}

static void handle_write_begin(int cfd, const char *ss_id, const char *client_id, const char *arg);
static void handle_write_update(int cfd, const char *ss_id, const char *client_id, const char *index_token, const char *arg);
static void handle_write_commit(int cfd, const char *ss_id, const char *client_id);
static void handle_undo(int cfd, const char *ss_id, const char *client_id, const char *arg);

// Helper to split a word if it contains delimiters
// Returns true if split happened (and w might be modified/next changed)
static bool word_check_split(WordNode *w, SentenceNode *s) {
    if (!w || !w->data) return false;
    char *p = w->data;
    size_t len = strlen(p);
    size_t split_idx = (size_t)-1;
    
    for (size_t i = 0; i < len; ++i) {
        if (is_sentence_delimiter(p[i])) {
            split_idx = i;
            break;
        }
    }

    if (split_idx == (size_t)-1) return false;

    // We have a delimiter at split_idx.
    // The part including the delimiter stays in w.
    // The part after the delimiter moves to a new word w_next.
    
    // If delimiter is at the very end, we don't need to split the word itself, 
    // but we need to signal that this word ends the sentence.
    if (split_idx == len - 1) {
        return true; // Signal that this word is a terminator
    }

    // Split "hello.world" -> "hello." and "world"
    char *first_part = strndup(p, split_idx + 1);
    char *second_part = strdup(p + split_idx + 1);
    
    free(w->data);
    w->data = first_part;
    
    WordNode *new_w = word_new(second_part);
    free(second_part);
    
    new_w->next = w->next;
    new_w->prev = w;
    if (w->next) w->next->prev = new_w;
    w->next = new_w;
    
    if (s->words_tail == w) s->words_tail = new_w;
    
    return true;
}

// Normalize sentence: check words for delimiters.
// If a word ends with a delimiter (or is split to end with one),
// split the sentence there.
static void sentence_normalize(Document *doc, SentenceNode *s) {
    if (!s) return;
    
    WordNode *cur = s->words_head;
    while (cur) {
        bool is_terminator = word_check_split(cur, s);
        if (is_terminator) {
            // cur ends with a delimiter.
            // All words after cur should move to a new sentence.
            WordNode *next_word = cur->next;
            
            if (next_word) {
                SentenceNode *new_s = sentence_new();
                
                // Move words
                new_s->words_head = next_word;
                new_s->words_tail = s->words_tail;
                
                next_word->prev = NULL;
                s->words_tail = cur;
                cur->next = NULL;
                
                // Link sentences
                new_s->next = s->next;
                new_s->prev = s;
                if (s->next) s->next->prev = new_s;
                s->next = new_s;
                
                if (doc->tail == s) doc->tail = new_s;
                doc->count++;
                
                // Continue normalizing the new sentence
                sentence_normalize(doc, new_s);
                return;
            } else {
                // Delimiter is at the last word.
                // If there are more sentences following, we are good.
                // If we just added a delimiter to the end, we might need to ensure
                // the next thing is a new sentence (which it is, by structure).
            }
        }
        cur = cur->next;
    }
}

static void handle_write_begin(int cfd, const char *ss_id, const char *client_id, const char *arg) {
    if (!arg || *arg == '\0') { dprintf(cfd, "ERR WRITE missing_arguments\n"); send_stop_packet(cfd, "WRITE", "-"); return; }

    char filename[ID_MAX];
    int sentence_index = 0;
    if (sscanf(arg, "%63s %d", filename, &sentence_index) != 2) { dprintf(cfd, "ERR WRITE bad_arguments\n"); send_stop_packet(cfd, "WRITE", "-"); return; }
    if (!is_valid_name_component(filename)) { dprintf(cfd, "ERR WRITE invalid_filename\n"); send_stop_packet(cfd, "WRITE", filename); return; }
    if (sentence_index <= 0) { dprintf(cfd, "ERR WRITE bad_sentence_index\n"); send_stop_packet(cfd, "WRITE", filename); return; }

    if (session_find_by_client(client_id)) { dprintf(cfd, "ERR WRITE already_in_progress\n"); send_stop_packet(cfd, "WRITE", filename); return; }

    log_message("INFO", "Client %s starting WRITE session on %s (sentence %d)", client_id, filename, sentence_index);

    Document *doc = get_document(filename);
    if (!doc) { dprintf(cfd, "ERR WRITE load_failed\n"); send_stop_packet(cfd, "WRITE", filename); return; }

    SentenceNode *target = doc->head;
    int idx = 1;
    while (target && idx < sentence_index) {
        target = target->next;
        idx++;
    }

    if (!target) {
        if (idx == sentence_index) {
            // Check if we can append a new sentence
            // We can only append (access n+1) if the last sentence ends with a delimiter
            if (doc->tail && !sentence_ends_with_delimiter(doc->tail)) {
                dprintf(cfd, "ERR WRITE sentence_out_of_range\n");
                send_stop_packet(cfd, "WRITE", filename);
                return;
            }
            // Append new sentence (blank line)
            target = sentence_new();
            document_append_sentence(doc, target);
        } else {
            dprintf(cfd, "ERR WRITE sentence_out_of_range\n");
            send_stop_packet(cfd, "WRITE", filename);
            return;
        }
    }

    if (target->locked) {
        if (strcmp(target->locked_by, client_id) != 0) {
            dprintf(cfd, "ERR WRITE locked %s %d\n", filename, sentence_index);
            send_stop_packet(cfd, "WRITE", filename);
            return;
        }
    }

    WriteSession *session = session_allocate();
    if (!session) { dprintf(cfd, "ERR WRITE too_many_sessions\n"); send_stop_packet(cfd, "WRITE", filename); return; }

    target->locked = true;
    strncpy(target->locked_by, client_id, ID_MAX-1);

    session->doc = doc;
    doc->ref_count++;
    session->target = target;
    strncpy(session->client_id, client_id, ID_MAX-1);
    strncpy(session->filename, filename, ID_MAX-1);
    
    // No draft copy needed, we operate on live structure
    session->draft_head = NULL;
    session->draft_tail = NULL;

    dprintf(cfd, "ACK %s WRITE READY %s %d\n", ss_id, filename, sentence_index);
    send_sentence_snapshot(cfd, session->target);
}

static void handle_write_update(int cfd, const char *ss_id, const char *client_id, const char *index_token, const char *arg) {
    if (!index_token || !is_number_string(index_token)) { 
        log_message("WARN", "Client %s sent bad word index: %s", client_id, index_token ? index_token : "(null)");
        dprintf(cfd, "ERR WRITE bad_word_index\n"); 
        return; 
    }
    if (!arg || *arg == '\0') { 
        log_message("WARN", "Client %s sent missing content for WRITE", client_id);
        dprintf(cfd, "ERR WRITE missing_content\n"); 
        return; 
    }

    long idx_long = strtol(index_token, NULL, 10);
    if (idx_long <= 0) { 
        log_message("WARN", "Client %s sent invalid word index: %ld", client_id, idx_long);
        dprintf(cfd, "ERR WRITE bad_word_index\n"); 
        return; 
    }
    int word_index = (int)idx_long;

    // log_message("DEBUG", "Client %s WRITE update %s word %d: %s", client_id, session->filename, word_index, arg);

    WriteSession *session = session_find_by_client(client_id);
    if (!session) { 
        log_message("WARN", "Client %s attempted WRITE update without active session", client_id);
        dprintf(cfd, "ERR WRITE no_active_session\n"); 
        return; 
    }

    SentenceNode *s = session->target;
    if (!s) { 
        log_message("ERROR", "Client %s session has invalid state (no target)", client_id);
        dprintf(cfd, "ERR WRITE invalid_session_state\n"); 
        return; 
    }

    int current_idx = 1;
    WordNode *w = s->words_head;
    
    while (w) {
        if (current_idx == word_index) {
            break;
        }
        w = w->next;
        current_idx++;
    }
    
    if (!w) {
        if (current_idx == word_index) {
            // Append word at the end
            sentence_append_word(s, arg);
            log_message("INFO", "Client %s appended word '%s' at index %d", client_id, arg, word_index);
        } else {
            log_message("WARN", "Client %s index out of bounds: %d (max %d)", client_id, word_index, current_idx);
            dprintf(cfd, "ERR WRITE index_out_of_bounds\n");
            return;
        }
    } else {
        // Insert new word BEFORE the word at word_index
        WordNode *new_word = word_new(arg);
        if (!new_word) {
            log_message("ERROR", "Allocation failed for new word");
            dprintf(cfd, "ERR WRITE allocation_failed\n");
            return;
        }
        
        // Insert new_word before w
        new_word->next = w;
        new_word->prev = w->prev;
        
        if (w->prev) {
            w->prev->next = new_word;
        } else {
            // w was the head, so new_word becomes the new head
            s->words_head = new_word;
        }
        w->prev = new_word;
        log_message("INFO", "Client %s inserted word '%s' at index %d", client_id, arg, word_index);
    }
    
    // Normalize sentence (handle splitting)
    sentence_normalize(session->doc, s);
    
    dprintf(cfd, "ACK %s WRITE UPDATED %s\n", ss_id, session->filename);
    send_sentence_snapshot(cfd, session->target);
}

static void handle_write_commit(int cfd, const char *ss_id, const char *client_id) {
    WriteSession *session = session_find_by_client(client_id);
    if (!session) { dprintf(cfd, "ERR WRITE no_active_session\n"); return; }

    log_message("INFO", "Client %s committing WRITE on %s", client_id, session->filename);

    Document *doc = session->doc;
    SentenceNode *target = session->target;
    
    // Unlock
    if (target) {
        target->locked = false;
        target->locked_by[0] = '\0';
    }
    
    session->target = NULL;
    
    char path[PATH_MAX];
    storage_path_for(session->filename, path, sizeof(path));
    char undo_path[PATH_MAX];
    undo_path_for(session->filename, undo_path, sizeof(undo_path));
    
    // Backup current file to undo path
    copy_file_to_path(path, undo_path);
    
    // Save document to disk
    write_document_to_path(doc, path, 0666);
    
    char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
    char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
    load_info_fields(session->filename, owner, sizeof(owner), access_str, sizeof(access_str), created, sizeof(created), lastmod, sizeof(lastmod), last_access_ts, sizeof(last_access_ts), last_access_user, sizeof(last_access_user));
    char now[64]; format_time_now(now, sizeof(now));
    write_info_txt(session->filename, owner[0]?owner:NULL, access_str, created[0]?created:NULL, now, now, client_id);

    dprintf(cfd, "ACK %s WRITE COMMIT %s\n", ss_id, session->filename);
    // Send STOP so the client knows the WRITE session is complete
    send_stop_packet(cfd, "WRITE", session->filename);
    session_release(session);
}

static void handle_undo(int cfd, const char *ss_id, const char *client_id, const char *arg) {
    char filename[ID_MAX];
    if (!arg || sscanf(arg, "%63s", filename) != 1) { dprintf(cfd, "ERR UNDO missing_filename\n"); return; }
    if (!is_valid_name_component(filename)) { dprintf(cfd, "ERR UNDO invalid_filename\n"); return; }
    if (file_has_active_session(filename)) { dprintf(cfd, "ERR UNDO write_in_progress\n"); return; }

    log_message("INFO", "Client %s requested UNDO on %s", client_id, filename);

    char current_path[PATH_MAX];
    if (storage_path_for(filename, current_path, sizeof(current_path)) < 0) { dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return; }
    struct stat st;
    if (stat(current_path, &st) < 0) { dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return; }

    char undo_path[PATH_MAX];
    if (undo_path_for(filename, undo_path, sizeof(undo_path)) < 0) { dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return; }
    if (stat(undo_path, &st) < 0) {
        if (errno == ENOENT) dprintf(cfd, "ERR UNDO no_history\n");
        else dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }

    char swap_path[PATH_MAX];
    if (swap_path_for(filename, swap_path, sizeof(swap_path)) < 0) { dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return; }

    unlink(swap_path);
    if (rename(current_path, swap_path) < 0) { dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return; }
    if (rename(undo_path, current_path) < 0) {
        int saved = errno; rename(swap_path, current_path); errno = saved;
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return;
    }
    if (rename(swap_path, undo_path) < 0) {
        int saved = errno; rename(current_path, undo_path); rename(swap_path, current_path); errno = saved;
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno)); return;
    }
    
    invalidate_document(filename);
    dprintf(cfd, "ACK %s UNDO OK %s\n", ss_id, filename);
}

#define MAX_DOCS 64
static Document *g_docs[MAX_DOCS];

static void handle_checkpoint(int cfd, const char *ss_id, const char *client_id, const char *filename, const char *tag) {
    if (!filename || !tag || !is_valid_name_component(filename)) {
        dprintf(cfd, "ERR CHECKPOINT invalid_args\n");
        return;
    }
    
    char src_path[PATH_MAX], ckpt_path[PATH_MAX];
    if (storage_path_for(filename, src_path, sizeof(src_path)) < 0) {
        dprintf(cfd, "ERR CHECKPOINT path_error\n");
        return;
    }
    if (checkpoint_path_for(filename, tag, ckpt_path, sizeof(ckpt_path)) < 0) {
        dprintf(cfd, "ERR CHECKPOINT checkpoint_path_error\n");
        return;
    }
    
    if (copy_file_to_path(src_path, ckpt_path) < 0) {
        dprintf(cfd, "ERR CHECKPOINT copy_failed %s\n", strerror(errno));
        log_message("ERROR", "[SS %s] Failed to create checkpoint %s for %s by %s: %s", 
                    ss_id, tag, filename, client_id, strerror(errno));
        return;
    }
    
    log_message("INFO", "[SS %s] Created checkpoint %s for %s by %s", ss_id, tag, filename, client_id);
    dprintf(cfd, "ACK %s CHECKPOINT %s %s\n", ss_id, filename, tag);
}

static void handle_viewcheckpoint(int cfd, const char *ss_id, const char *client_id, const char *filename, const char *tag) {
    if (!filename || !tag || !is_valid_name_component(filename)) {
        dprintf(cfd, "ERR VIEWCHECKPOINT invalid_args\n");
        return;
    }
    
    char ckpt_path[PATH_MAX];
    if (checkpoint_path_for(filename, tag, ckpt_path, sizeof(ckpt_path)) < 0) {
        dprintf(cfd, "ERR VIEWCHECKPOINT path_error\n");
        return;
    }
    
    int fd = open(ckpt_path, O_RDONLY);
    if (fd < 0) {
        dprintf(cfd, "ERR VIEWCHECKPOINT notfound %s\n", strerror(errno));
        log_message("ERROR", "[SS %s] Checkpoint %s for %s not found by %s", ss_id, tag, filename, client_id);
        return;
    }
    
    struct stat st;
    long long size = -1;
    if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) size = st.st_size;
    
    dprintf(cfd, "DATA %s VIEWCHECKPOINT %s %s %lld\n", ss_id, filename, tag, size);
    
    char buffer[4096];
    ssize_t rbytes;
    int send_failed = 0;
    while ((rbytes = read(fd, buffer, sizeof(buffer))) > 0) {
        if (send_all(cfd, buffer, (size_t)rbytes) < 0) {
            send_failed = 1;
            log_message("ERROR", "[SS %s] send failure for checkpoint %s: %s", ss_id, tag, strerror(errno));
            break;
        }
    }
    
    if (rbytes < 0 && !send_failed) {
        dprintf(cfd, "\nERR VIEWCHECKPOINT read_error\n");
    } else if (!send_failed) {
        dprintf(cfd, "\nENDDATA %s VIEWCHECKPOINT %s %s\n", ss_id, filename, tag);
        log_message("INFO", "[SS %s] Sent checkpoint %s for %s to %s", ss_id, tag, filename, client_id);
    }
    
    close(fd);
}

static void handle_revert(int cfd, const char *ss_id, const char *client_id, const char *filename, const char *tag) {
    if (!filename || !tag || !is_valid_name_component(filename)) {
        dprintf(cfd, "ERR REVERT invalid_args\n");
        return;
    }
    
    char ckpt_path[PATH_MAX], dest_path[PATH_MAX], undo_path[PATH_MAX];
    if (checkpoint_path_for(filename, tag, ckpt_path, sizeof(ckpt_path)) < 0) {
        dprintf(cfd, "ERR REVERT checkpoint_path_error\n");
        return;
    }
    if (storage_path_for(filename, dest_path, sizeof(dest_path)) < 0) {
        dprintf(cfd, "ERR REVERT storage_path_error\n");
        return;
    }
    
    // Check if checkpoint exists
    struct stat st;
    if (stat(ckpt_path, &st) != 0) {
        dprintf(cfd, "ERR REVERT checkpoint_notfound %s\n", strerror(errno));
        log_message("ERROR", "[SS %s] Checkpoint %s for %s not found by %s", ss_id, tag, filename, client_id);
        return;
    }
    
    // CRITICAL: Create undo backup BEFORE reverting (so UNDO can revert the REVERT)
    if (undo_path_for(filename, undo_path, sizeof(undo_path)) == 0) {
        // Check if current file exists
        if (stat(dest_path, &st) == 0) {
            // Backup current state to undo file before reverting
            if (copy_file_to_path(dest_path, undo_path) < 0) {
                log_message("WARN", "[SS %s] Failed to create undo backup for %s before REVERT by %s",
                           ss_id, filename, client_id);
                // Continue with revert even if undo backup fails
            } else {
                log_message("INFO", "[SS %s] Created undo backup for %s before REVERT to %s by %s",
                           ss_id, filename, tag, client_id);
            }
        }
    }
    
    // Now perform the revert: copy checkpoint to actual file
    if (copy_file_to_path(ckpt_path, dest_path) < 0) {
        dprintf(cfd, "ERR REVERT copy_failed %s\n", strerror(errno));
        log_message("ERROR", "[SS %s] Failed to revert %s to checkpoint %s by %s: %s",
                    ss_id, filename, tag, client_id, strerror(errno));
        return;
    }
    
    // Update metadata with new timestamp
    char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
    char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
    load_info_fields(filename,
                     owner, sizeof(owner),
                     access_str, sizeof(access_str),
                     created, sizeof(created),
                     lastmod, sizeof(lastmod),
                     last_access_ts, sizeof(last_access_ts),
                     last_access_user, sizeof(last_access_user));
    char now[64];
    format_time_now(now, sizeof(now));
    write_info_txt(filename,
                   owner[0]?owner:NULL,
                   access_str,
                   created[0]?created:NULL,
                   now,  // Update last modified time
                   now,  // Update last access time
                   client_id);
    
    // Invalidate in-memory document cache
    invalidate_document(filename);
    
    log_message("INFO", "[SS %s] Reverted %s to checkpoint %s by %s", ss_id, filename, tag, client_id);
    dprintf(cfd, "ACK %s REVERT %s %s\n", ss_id, filename, tag);
}

static void handle_listcheckpoints(int cfd, const char *ss_id, const char *client_id, const char *filename) {
    if (!filename || !is_valid_name_component(filename)) {
        dprintf(cfd, "ERR LISTCHECKPOINTS invalid_filename\n");
        return;
    }
    
    if (ensure_checkpoints_dir() < 0) {
        dprintf(cfd, "ERR LISTCHECKPOINTS no_checkpoint_dir\n");
        return;
    }
    
    char ckpt_dir[PATH_MAX];
    snprintf(ckpt_dir, sizeof(ckpt_dir), "%s/checkpoints", g_storage_dir);
    
    DIR *d = opendir(ckpt_dir);
    if (!d) {
        dprintf(cfd, "ERR LISTCHECKPOINTS cannot_open_dir %s\n", strerror(errno));
        return;
    }
    
    char prefix[FNAME_MAX + 2];
    snprintf(prefix, sizeof(prefix), "%s_", filename);
    size_t prefix_len = strlen(prefix);
    
    dprintf(cfd, "CHECKPOINTS %s\n", filename);
    
    struct dirent *entry;
    int count = 0;
    while ((entry = readdir(d)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        if (strncmp(entry->d_name, prefix, prefix_len) == 0) {
            const char *tag = entry->d_name + prefix_len;
            dprintf(cfd, "  %s\n", tag);
            count++;
        }
    }
    
    closedir(d);
    
    if (count == 0) {
        dprintf(cfd, "  (no checkpoints)\n");
    }
    
    dprintf(cfd, "END_CHECKPOINTS %s\n", filename);
    log_message("INFO", "[SS %s] Listed %d checkpoints for %s to %s", ss_id, count, filename, client_id);
}

static Document *get_document(const char *filename) {
    for (int i = 0; i < MAX_DOCS; ++i) {
        if (g_docs[i] && strcmp(g_docs[i]->filename, filename) == 0) {
            return g_docs[i];
        }
    }
    for (int i = 0; i < MAX_DOCS; ++i) {
        if (!g_docs[i]) {
            Document *doc = malloc(sizeof(Document));
            if (!doc) return NULL;
            if (document_load_from_file(filename, doc, NULL) < 0) { free(doc); return NULL; }
            g_docs[i] = doc;
            return doc;
        }
    }
    return NULL;
}

static void invalidate_document(const char *filename) {
    for (int i = 0; i < MAX_DOCS; ++i) {
        if (g_docs[i] && strcmp(g_docs[i]->filename, filename) == 0) {
            document_free_content(g_docs[i]);
            free(g_docs[i]);
            g_docs[i] = NULL;
            return;
        }
    }
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);

    if (argc == 3) {
        strncpy(g_nm_ip, argv[1], sizeof(g_nm_ip) - 1);
        g_nm_port = atoi(argv[2]);
    } else if (argc != 1) {
        fprintf(stderr, "Usage: %s [nm_ip nm_port]\n", argv[0]);
        return 1;
    }

    char ss_id[ID_MAX];
    int client_port;

    printf("|Storage server initialisation| Enter storageserver_id: ");
    fflush(stdout);
    if (scanf("%63s", ss_id) != 1) { fprintf(stderr, "bad id\n"); return 1; }

    if (ensure_storage_directory(ss_id) != 0) {
        log_message("ERROR", "storage directory doesnot exist in function ss.main");
        perror("storage directory doesnot exist in function ss.main");
        return 1;
    }

    printf("|Storage server initialisation| Enter port to listen on: ");
    fflush(stdout);
    if (scanf("%d", &client_port) != 1) { fprintf(stderr, "bad port\n"); return 1; }

    // Drain leftover newline from stdin so later fgets/reads aren't polluted
    int ch; while ((ch = getchar()) != '\n' && ch != EOF) {}

    int listen_fd = create_listen_socket(client_port);
    log_message("INFO", "[Storage Server %s] is now listening for clients on port %d", ss_id, client_port);

    // Register with Name Server
    int nmfd = connect_to_nm(g_nm_ip, g_nm_port);
    dprintf(nmfd, "REGISTER %s %d\n", ss_id, client_port);
    char resp[LINE_MAX]; read_line(nmfd, resp, sizeof(resp));
    log_message("INFO", "[SS %s] Named Server response: %s", ss_id, resp);
    close(nmfd);

    send_existing_files_to_nm(ss_id);

    // Accept clients and print debug on connect
    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            log_message("ERROR", "accept function error in function ss.main: %s", strerror(errno)); continue;
        }
        char line[LINE_MAX];
        ssize_t n = read_line(cfd, line, sizeof(line));
        if (n > 0) {
            // Expect: HELLO <client_id> <command...>
            char client_id[ID_MAX] = {0};
            char cmd[LINE_MAX] = {0};
            if (sscanf(line, "HELLO %63s %2047[^\n]", client_id, cmd) >= 1) {
                log_message("INFO", "Storage Server %s connected to Client %s", ss_id, client_id);
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
                        } else {
                            char fname[ID_MAX]={0}, owner_token[ID_MAX]={0};
                            int got = sscanf(arg, "%63s %63s", fname, owner_token);
                            if (got < 1 || !is_valid_name_component(fname)) {
                                dprintf(cfd, "ERR CREATE invalid_filename\n");
                            } else {
                                int rc = create_storage_file(fname);
                                if (rc == 0) {
                                    // create metadata dir and write initial info.txt
                                    char created[64], lastmod[64];
                                    format_time_now(created, sizeof(created));
                                    strncpy(lastmod, created, sizeof(lastmod)-1);
                                    char access_str[256]; access_str[0]='\0';
                                    if (got == 2 && owner_token[0]) {
                                        snprintf(access_str, sizeof(access_str), "%s (RW)", owner_token);
                                    }
                                    write_info_txt(fname,
                                                   (got==2)?owner_token:NULL,
                                                   access_str,
                                                   created,
                                                   lastmod,
                                                   created,
                                                   (got==2)?owner_token:NULL);
                                    log_message("INFO", "[SS %s] Successful Create -> file %s | Owner -> %s", ss_id, fname, client_id);
                                    dprintf(cfd, "ACK %s CREATE OK %s\n", ss_id, fname);
                                } else {
                                    int err = errno;
                                    log_message("ERROR", "[SS %s] Create file failed | %s not created | client %s: %s",
                                            ss_id, fname, client_id, strerror(err));
                                    if (err == EEXIST) {
                                        dprintf(cfd, "ERR CREATE EXISTS %s\n", fname);
                                    } else {
                                        dprintf(cfd, "ERR CREATE %s\n", strerror(err));
                                    }
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
                                log_message("INFO", "[SS %s] Successfully deleted file %s by Client %s", ss_id, arg, client_id);
                                dprintf(cfd, "ACK %s DELETE OK %s\n", ss_id, arg);
                            } else {
                                int err = errno;
                                log_message("ERROR", "[SS %s] failed to delete file %s for client %s: %s",
                                        ss_id, arg, client_id, strerror(err));
                                if (err == ENOENT) {
                                    dprintf(cfd, "ERR DELETE NOTFOUND %s\n", arg);
                                } else {
                                    dprintf(cfd, "ERR DELETE %s\n", strerror(err));
                                }
                            }
                        }
                    } else if (strcmp(verb_upper, "READ") == 0) {
                        const char *read_name = (arg && arg[0]) ? arg : "-";
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR READ missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR READ invalid_filename\n");
                        } else {
                            int fd = open_storage_file_ro(arg);
                            if (fd < 0) {
                                int err = errno;
                                log_message("ERROR", "[SS %s] failed to open file %s for client %s: %s",
                                        ss_id, arg, client_id, strerror(err));
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
                                        log_message("ERROR", "[SS %s] send failure while streaming %s to client %s: %s",
                                                ss_id, arg, client_id, strerror(err));
                                        break;
                                    }
                                }
                                if (rbytes < 0) {
                                    int err = errno;
                                    log_message("ERROR", "[SS %s] read failure while streaming %s to client %s: %s",
                                            ss_id, arg, client_id, strerror(err));
                                    if (!send_failed) {
                                        dprintf(cfd, "\nERR READ IO %s\n", strerror(err));
                                    }
                                } else if (!send_failed) {
                                    dprintf(cfd, "\nENDDATA %s READ %s\n", ss_id, arg);
                                    log_message("INFO", "[SS %s] Successful Streaming of file %s to client %s", ss_id, arg, client_id);
                                    // update info.txt last accessed
                                    char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
                                    char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                                    load_info_fields(arg,
                                                     owner, sizeof(owner),
                                                     access_str, sizeof(access_str),
                                                     created, sizeof(created),
                                                     lastmod, sizeof(lastmod),
                                                     last_access_ts, sizeof(last_access_ts),
                                                     last_access_user, sizeof(last_access_user));
                                    char now[64]; format_time_now(now, sizeof(now));
                                    write_info_txt(arg,
                                                   owner[0]?owner:NULL,
                                                   access_str,
                                                   created[0]?created:NULL,
                                                   lastmod[0]?lastmod:NULL,
                                                   now,
                                                   client_id);
                                }
                                close(fd);
                            }
                        }
                        send_stop_packet(cfd, "READ", read_name);
                    } else if (strcmp(verb_upper, "STREAM") == 0) {
                        const char *stream_name = (arg && arg[0]) ? arg : "-";
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR STREAM missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR STREAM invalid_filename\n");
                        } else {
                            char path[PATH_MAX];
                            if (storage_path_for(arg, path, sizeof(path)) < 0) {
                                dprintf(cfd, "ERR STREAM %s\n", strerror(errno));
                            } else {
                                FILE *f = fopen(path, "r");
                                if (!f) {
                                    dprintf(cfd, "ERR STREAM %s\n", strerror(errno));
                                } else {
                                    log_message("INFO", "[SS %s] Starting streaming file %s to client %s", ss_id, arg, client_id);
                                    char word[1024];
                                    bool send_error = false;
                                    while (fscanf(f, "%1023s", word) == 1) {
                                        size_t len = strlen(word);
                                        if (send_all(cfd, word, len) < 0 || send_all(cfd, " ", 1) < 0) {
                                            send_error = true;
                                            int err = errno;
                                            log_message("ERROR", "[SS %s] client %s disconnected mid-stream for %s: %s",
                                                    ss_id, client_id, arg, strerror(err));
                                            break;
                                        }
                                        usleep(100000);
                                    }
                                    fclose(f);
                                    if (!send_error) {
                                        send_all(cfd, "\n", 1);
                                        char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
                                        char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                                        load_info_fields(arg,
                                                         owner, sizeof(owner),
                                                         access_str, sizeof(access_str),
                                                         created, sizeof(created),
                                                         lastmod, sizeof(lastmod),
                                                         last_access_ts, sizeof(last_access_ts),
                                                         last_access_user, sizeof(last_access_user));
                                        char now[64]; format_time_now(now, sizeof(now));
                                        write_info_txt(arg,
                                                       owner[0]?owner:NULL,
                                                       access_str,
                                                       created[0]?created:NULL,
                                                       lastmod[0]?lastmod:NULL,
                                                       now,
                                                       client_id);
                                    }
                                }
                            }
                        }
                        send_stop_packet(cfd, "STREAM", stream_name);
                    } else if (strcmp(verb_upper, "EXEC") == 0) {
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR EXEC missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR EXEC invalid_filename\n");
                        } else {
                            int fd = open_storage_file_ro(arg);
                            if (fd < 0) {
                                int err = errno;
                                log_message("ERROR", "[SS %s] failed to open file %s for EXEC by %s: %s",
                                        ss_id, arg, client_id, strerror(err));
                                if (err == ENOENT) dprintf(cfd, "ERR EXEC NOTFOUND %s\n", arg);
                                else dprintf(cfd, "ERR EXEC %s\n", strerror(err));
                            } else {
                                struct stat st;
                                long long declared_size = -1;
                                if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) declared_size = st.st_size;
                                dprintf(cfd, "DATA %s EXEC %s %lld\n", ss_id, arg, declared_size);
                                char buffer[4096]; ssize_t rbytes; int send_failed=0;
                                while ((rbytes = read(fd, buffer, sizeof(buffer))) > 0) {
                                    if (send_all(cfd, buffer, (size_t)rbytes) < 0) { send_failed=1; break; }
                                }
                                if (rbytes < 0) {
                                    int err = errno; if (!send_failed) dprintf(cfd, "\nERR EXEC IO %s\n", strerror(err));
                                } else if (!send_failed) {
                                    dprintf(cfd, "\nENDDATA %s EXEC %s\n", ss_id, arg);
                                    log_message("INFO", "[SS %s] Successfully sent EXEC payload %s to %s", ss_id, arg, client_id);
                                }
                                close(fd);
                                // update last access
                                char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
                                char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                                load_info_fields(arg,
                                                 owner, sizeof(owner),
                                                 access_str, sizeof(access_str),
                                                 created, sizeof(created),
                                                 lastmod, sizeof(lastmod),
                                                 last_access_ts, sizeof(last_access_ts),
                                                 last_access_user, sizeof(last_access_user));
                                char now[64]; format_time_now(now, sizeof(now));
                                write_info_txt(arg,
                                               owner[0]?owner:NULL,
                                               access_str,
                                               created[0]?created:NULL,
                                               lastmod[0]?lastmod:NULL,
                                               now,
                                               client_id);
                            }
                        }
                    } else if (strcmp(verb_upper, "INFO") == 0) {
                        if (!arg || arg[0] == '\0') { dprintf(cfd, "ERR INFO missing_filename\n"); }
                        else if (!is_valid_name_component(arg)) { dprintf(cfd, "ERR INFO invalid_filename\n"); }
                        else {
                            char dir[PATH_MAX];
                            if (meta_dir_for(arg, dir, sizeof(dir)) < 0) { dprintf(cfd, "ERR INFO no_metadata %s\n", arg); }
                            else {
                                char info_path[PATH_MAX]; snprintf(info_path, sizeof(info_path), "%s/info.txt", dir);
                                FILE *f = fopen(info_path, "r");
                                if (!f) { dprintf(cfd, "ERR INFO missing %s\n", arg); }
                                else { char line[1024]; while (fgets(line, sizeof(line), f)) send_all(cfd, line, strlen(line)); fclose(f); }
                            }
                        }
                    } else if (strcmp(verb_upper, "ADDACCESS") == 0) {
                        char flag[16]={0}, fname[ID_MAX]={0}, user[ID_MAX]={0};
                        if (!arg || sscanf(arg, "%15s %63s %63s", flag, fname, user) != 3) { dprintf(cfd, "ERR ADDACCESS bad_args\n"); }
                        else {
                            char owner[ID_MAX]={0}, access_str[512]={0}, created[64]={0}, lastmod[64]={0};
                            char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                            load_info_fields(fname,
                                             owner, sizeof(owner),
                                             access_str, sizeof(access_str),
                                             created, sizeof(created),
                                             lastmod, sizeof(lastmod),
                                             last_access_ts, sizeof(last_access_ts),
                                             last_access_user, sizeof(last_access_user));
                            
                            char new_access[1024]; new_access[0]='\0';
                            char acc_copy[512]; strncpy(acc_copy, access_str, sizeof(acc_copy)-1); acc_copy[sizeof(acc_copy)-1]='\0';
                            
                            bool found = false;
                            bool first = true;
                            char *tok = strtok(acc_copy, ",");
                            while(tok){
                                while(isspace((unsigned char)*tok)) tok++;
                                char u[ID_MAX]={0}, p[16]={0};
                                char *paren = strchr(tok, '(');
                                if(paren){
                                    size_t ulen = paren - tok;
                                    if(ulen >= ID_MAX) ulen = ID_MAX-1;
                                    strncpy(u, tok, ulen); u[ulen]='\0';
                                    while(ulen>0 && isspace((unsigned char)u[ulen-1])) u[--ulen]='\0';
                                    
                                    char *endp = strchr(paren, ')');
                                    if(endp){
                                        size_t plen = endp - (paren+1);
                                        if(plen >= sizeof(p)) plen = sizeof(p)-1;
                                        strncpy(p, paren+1, plen); p[plen]='\0';
                                    }
                                } else {
                                    strncpy(u, tok, ID_MAX-1);
                                }

                                if(strcmp(u, user)==0){
                                    found = true;
                                    bool has_r = (strchr(p, 'R') != NULL);
                                    bool has_w = (strchr(p, 'W') != NULL);
                                    if(strcmp(flag, "-R")==0) has_r = true;
                                    if(strcmp(flag, "-W")==0) {
                                        has_w = true;
                                        has_r = true;  // Write access implies read access
                                    }
                                    
                                    if(!first) strcat(new_access, ", ");
                                    strcat(new_access, user);
                                    strcat(new_access, " (");
                                    if(has_r) strcat(new_access, "R");
                                    if(has_w) strcat(new_access, "W");
                                    strcat(new_access, ")");
                                } else {
                                    if(!first) strcat(new_access, ", ");
                                    strcat(new_access, tok);
                                }
                                first = false;
                                tok = strtok(NULL, ",");
                            }

                            if(!found){
                                if(!first) strcat(new_access, ", ");
                                strcat(new_access, user);
                                strcat(new_access, " (");
                                if(strcmp(flag, "-R")==0) strcat(new_access, "R");
                                else if(strcmp(flag, "-W")==0) strcat(new_access, "RW");  // Write implies read
                                else strcat(new_access, "RW");
                                strcat(new_access, ")");
                            }
                            
                            write_info_txt(fname,
                                           owner[0]?owner:NULL,
                                           new_access,
                                           created[0]?created:NULL,
                                           lastmod[0]?lastmod:NULL,
                                           last_access_ts[0]?last_access_ts:NULL,
                                           last_access_user[0]?last_access_user:NULL);
                            dprintf(cfd, "ACK %s ADDACCESS %s %s %s\n", ss_id, flag, fname, user);
                        }
                    } else if (strcmp(verb_upper, "REMACCESS") == 0) {
                        char fname[ID_MAX]={0}, user[ID_MAX]={0};
                        if (!arg || sscanf(arg, "%63s %63s", fname, user) != 2) { dprintf(cfd, "ERR REMACCESS bad_args\n"); }
                        else {
                            char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
                            char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                            load_info_fields(fname,
                                             owner, sizeof(owner),
                                             access_str, sizeof(access_str),
                                             created, sizeof(created),
                                             lastmod, sizeof(lastmod),
                                             last_access_ts, sizeof(last_access_ts),
                                             last_access_user, sizeof(last_access_user));
                            char tmp[512]; tmp[0]='\0';
                            char acc_copy[512]; strncpy(acc_copy, access_str, sizeof(acc_copy)-1); acc_copy[sizeof(acc_copy)-1]='\0';
                            char *tok = strtok(acc_copy, ","); bool first=true;
                            while (tok) {
                                if (strstr(tok, user) == NULL) {
                                    if (!first) strncat(tmp, ",", sizeof(tmp)-strlen(tmp)-1);
                                    strncat(tmp, tok, sizeof(tmp)-strlen(tmp)-1);
                                    first=false;
                                }
                                tok = strtok(NULL, ",");
                            }
                            strncpy(access_str, tmp, sizeof(access_str)-1); access_str[sizeof(access_str)-1]='\0';
                            write_info_txt(fname,
                                           owner[0]?owner:NULL,
                                           access_str,
                                           created[0]?created:NULL,
                                           lastmod[0]?lastmod:NULL,
                                           last_access_ts[0]?last_access_ts:NULL,
                                           last_access_user[0]?last_access_user:NULL);
                            dprintf(cfd, "ACK %s REMACCESS %s %s\n", ss_id, fname, user);
                        }
                    } else if (strcmp(verb_upper, "WRITE") == 0) {
                        handle_write_begin(cfd, ss_id, client_id, arg);
                    } else if (strcmp(verb_upper, "ETIRW") == 0) {
                        handle_write_commit(cfd, ss_id, client_id);
                    } else if (strcmp(verb_upper, "UNDO") == 0) {
                        handle_undo(cfd, ss_id, client_id, arg);
                    } else if (strcmp(verb_upper, "CHECKPOINT") == 0) {
                        char fname[FNAME_MAX]={0}, tag[ID_MAX]={0};
                        if (arg && sscanf(arg, "%255s %63s", fname, tag) == 2) {
                            handle_checkpoint(cfd, ss_id, client_id, fname, tag);
                        } else {
                            dprintf(cfd, "ERR CHECKPOINT missing_args\n");
                        }
                    } else if (strcmp(verb_upper, "VIEWCHECKPOINT") == 0) {
                        char fname[FNAME_MAX]={0}, tag[ID_MAX]={0};
                        if (arg && sscanf(arg, "%255s %63s", fname, tag) == 2) {
                            handle_viewcheckpoint(cfd, ss_id, client_id, fname, tag);
                        } else {
                            dprintf(cfd, "ERR VIEWCHECKPOINT missing_args\n");
                        }
                    } else if (strcmp(verb_upper, "REVERT") == 0) {
                        char fname[FNAME_MAX]={0}, tag[ID_MAX]={0};
                        if (arg && sscanf(arg, "%255s %63s", fname, tag) == 2) {
                            handle_revert(cfd, ss_id, client_id, fname, tag);
                        } else {
                            dprintf(cfd, "ERR REVERT missing_args\n");
                        }
                    } else if (strcmp(verb_upper, "LISTCHECKPOINTS") == 0) {
                        if (arg && arg[0]) {
                            handle_listcheckpoints(cfd, ss_id, client_id, arg);
                        } else {
                            dprintf(cfd, "ERR LISTCHECKPOINTS missing_filename\n");
                        }
                    } else if (is_number_string(verb)) {
                        handle_write_update(cfd, ss_id, client_id, verb, arg);
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
