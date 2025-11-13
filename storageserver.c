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

#define NM_IP   "127.0.0.1"
#define NM_PORT 5000
#define ID_MAX  64

static char g_storage_dir[PATH_MAX];

static int storage_dir_for(const char *name, char *out, size_t out_sz);
static int ensure_file_container(const char *name);
static int storage_path_for(const char *name, char *out, size_t out_sz);
static int undo_path_for(const char *name, char *out, size_t out_sz);
static int swap_path_for(const char *name, char *out, size_t out_sz);

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

typedef struct {
    char **data;
    size_t count;
    size_t capacity;
} WordArray;

typedef struct {
    WordArray words;
    char *tail;
} Sentence;

typedef struct {
    Sentence *items;
    size_t count;
    size_t capacity;
} Document;

typedef struct {
    bool in_use;
    char client_id[ID_MAX];
    char filename[ID_MAX];
    int sentence_index;
    mode_t original_mode;
    Document doc;
} WriteSession;

static WordArray *word_array_init(WordArray *wa) {
    wa->data = NULL;
    wa->count = 0;
    wa->capacity = 0;
    return wa;
}

static void word_array_free(WordArray *wa) {
    if (!wa) return;
    for (size_t i = 0; i < wa->count; ++i) {
        free(wa->data[i]);
    }
    free(wa->data);
    wa->data = NULL;
    wa->count = 0;
    wa->capacity = 0;
}

static int word_array_reserve(WordArray *wa, size_t needed) {
    if (wa->capacity >= needed) {
        return 0;
    }
    size_t new_cap = wa->capacity ? wa->capacity * 2 : 8;
    while (new_cap < needed) new_cap *= 2;
    char **tmp = realloc(wa->data, new_cap * sizeof(*tmp));
    if (!tmp) return -1;
    wa->data = tmp;
    wa->capacity = new_cap;
    return 0;
}

static int word_array_append_slice(WordArray *wa, const char *start, size_t len) {
    if (word_array_reserve(wa, wa->count + 1) < 0) return -1;
    char *dup = strndup(start, len);
    if (!dup) return -1;
    wa->data[wa->count++] = dup;
    return 0;
}

static int word_array_append_string(WordArray *wa, const char *value) {
    return word_array_append_slice(wa, value, strlen(value));
}

static int word_array_replace(WordArray *wa, size_t index, const char *value) {
    if (index >= wa->count) {
        errno = EINVAL;
        return -1;
    }
    char *dup = strdup(value);
    if (!dup) return -1;
    free(wa->data[index]);
    wa->data[index] = dup;
    return 0;
}

static int sentence_init(Sentence *s) {
    if (!s) return -1;
    word_array_init(&s->words);
    s->tail = strdup("");
    if (!s->tail) return -1;
    return 0;
}

static void sentence_free(Sentence *s) {
    if (!s) return;
    word_array_free(&s->words);
    free(s->tail);
    s->tail = NULL;
}

static int sentence_set_tail(Sentence *s, const char *start, size_t len) {
    char *dup = strndup(start, len);
    if (!dup) return -1;
    free(s->tail);
    s->tail = dup;
    return 0;
}

static void document_init(Document *doc) {
    doc->items = NULL;
    doc->count = 0;
    doc->capacity = 0;
}

static void document_free(Document *doc) {
    if (!doc) return;
    for (size_t i = 0; i < doc->count; ++i) {
        sentence_free(&doc->items[i]);
    }
    free(doc->items);
    doc->items = NULL;
    doc->count = 0;
    doc->capacity = 0;
}

static Sentence *document_new_sentence(Document *doc) {
    if (doc->count == doc->capacity) {
        size_t new_cap = doc->capacity ? doc->capacity * 2 : 8;
        Sentence *tmp = realloc(doc->items, new_cap * sizeof(*tmp));
        if (!tmp) return NULL;
        doc->items = tmp;
        doc->capacity = new_cap;
    }
    Sentence *s = &doc->items[doc->count];
    if (sentence_init(s) < 0) return NULL;
    doc->count++;
    return s;
}

static bool is_sentence_delimiter(char c) {
    return c == '.' || c == '!' || c == '?' || c == '\n' || c == '\r';
}

static int document_parse_from_path(const char *path, Document *doc, mode_t *mode_out) {
    document_init(doc);

    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }
    if (mode_out) *mode_out = st.st_mode;

    size_t size = (size_t)st.st_size;
    char *buffer = malloc(size + 1);
    if (!buffer) {
        close(fd);
        errno = ENOMEM;
        return -1;
    }

    size_t off = 0;
    while (off < size) {
        ssize_t r = read(fd, buffer + off, size - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            free(buffer);
            close(fd);
            return -1;
        }
        if (r == 0) break;
        off += (size_t)r;
    }
    size = off;
    buffer[off] = '\0';
    close(fd);

    Sentence *current = NULL;
    size_t word_start = (size_t)-1;

    for (size_t i = 0; i <= size; ++i) {
        char c = (i < size) ? buffer[i] : '\0';
        bool at_end = (i == size);
    bool delim = (!at_end && is_sentence_delimiter(c));
    bool space = (!at_end && isspace((unsigned char)c) && c != '\n' && c != '\r');

        if (word_start == (size_t)-1) {
            if (!at_end && !delim && !space) {
                if (!current) {
                    current = document_new_sentence(doc);
                    if (!current) {
                        free(buffer);
                        document_free(doc);
                        errno = ENOMEM;
                        return -1;
                    }
                }
                word_start = i;
            }
        }

        if (word_start != (size_t)-1 && (at_end || delim || space)) {
            if (!current) {
                current = document_new_sentence(doc);
                if (!current) {
                    free(buffer);
                    document_free(doc);
                    errno = ENOMEM;
                    return -1;
                }
            }
            size_t len = i - word_start;
            if (len > 0) {
                if (word_array_append_slice(&current->words, buffer + word_start, len) < 0) {
                    free(buffer);
                    document_free(doc);
                    return -1;
                }
            }
            word_start = (size_t)-1;
        }

        if (delim) {
            if (!current) {
                current = document_new_sentence(doc);
                if (!current) {
                    free(buffer);
                    document_free(doc);
                    errno = ENOMEM;
                    return -1;
                }
            }
            size_t tail_start = i;
            size_t j = i + 1;
            while (j < size && isspace((unsigned char)buffer[j]) && buffer[j] != '\n' && buffer[j] != '\r') ++j;
            size_t tail_len = j - tail_start;
            if (sentence_set_tail(current, buffer + tail_start, tail_len) < 0) {
                free(buffer);
                document_free(doc);
                return -1;
            }
            current = NULL;
            i = j - 1;
        } else if (at_end) {
            if (current && !current->tail) {
                current->tail = strdup("");
            }
            current = NULL;
        }
    }

    free(buffer);
    return 0;
}

static char *sentence_join_words(const Sentence *s) {
    size_t total = 0;
    if (s->words.count > 0) {
        for (size_t i = 0; i < s->words.count; ++i) {
            total += strlen(s->words.data[i]);
        }
        total += (s->words.count - 1);
    }
    char *out = malloc(total + 1);
    if (!out) return NULL;
    char *ptr = out;
    for (size_t i = 0; i < s->words.count; ++i) {
        size_t len = strlen(s->words.data[i]);
        memcpy(ptr, s->words.data[i], len);
        ptr += len;
        if (i + 1 < s->words.count) *ptr++ = ' ';
    }
    *ptr = '\0';
    return out;
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

    for (size_t i = 0; i < doc->count; ++i) {
        const Sentence *s = &doc->items[i];
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
        free(joined);
        size_t tail_len = s->tail ? strlen(s->tail) : 0;
        if (tail_len > 0) {
            if (write_all_fd(tmp_fd, s->tail, tail_len) < 0) {
                int saved = errno;
                close(tmp_fd);
                unlink(tmp_template);
                errno = saved;
                return -1;
            }
        }
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
    for (size_t i = 0; i < sizeof(g_write_sessions) / sizeof(g_write_sessions[0]); ++i) {
        if (g_write_sessions[i].in_use && strcmp(g_write_sessions[i].client_id, client_id) == 0) {
            return &g_write_sessions[i];
        }
    }
    return NULL;
}

static WriteSession *session_find_lock(const char *filename, int sentence_index) {
    for (size_t i = 0; i < sizeof(g_write_sessions) / sizeof(g_write_sessions[0]); ++i) {
        if (!g_write_sessions[i].in_use) continue;
        if (g_write_sessions[i].sentence_index == sentence_index &&
            strcmp(g_write_sessions[i].filename, filename) == 0) {
            return &g_write_sessions[i];
        }
    }
    return NULL;
}

static WriteSession *session_allocate(void) {
    for (size_t i = 0; i < sizeof(g_write_sessions) / sizeof(g_write_sessions[0]); ++i) {
        if (!g_write_sessions[i].in_use) {
            g_write_sessions[i].in_use = true;
            document_init(&g_write_sessions[i].doc);
            g_write_sessions[i].client_id[0] = '\0';
            g_write_sessions[i].filename[0] = '\0';
            g_write_sessions[i].sentence_index = 0;
            g_write_sessions[i].original_mode = 0;
            return &g_write_sessions[i];
        }
    }
    return NULL;
}

static void session_release(WriteSession *session) {
    if (!session) return;
    document_free(&session->doc);
    session->in_use = false;
    session->client_id[0] = '\0';
    session->filename[0] = '\0';
    session->sentence_index = 0;
    session->original_mode = 0;
}

static Sentence *session_target_sentence(WriteSession *session) {
    if (!session) return NULL;
    if (session->sentence_index <= 0) return NULL;
    size_t idx = (size_t)(session->sentence_index - 1);
    if (idx >= session->doc.count) return NULL;
    return &session->doc.items[idx];
}

static bool file_has_active_session(const char *filename) {
    for (size_t i = 0; i < sizeof(g_write_sessions) / sizeof(g_write_sessions[0]); ++i) {
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

static void send_sentence_snapshot(int cfd, const Sentence *sentence) {
    char *joined = sentence_join_words(sentence);
    if (!joined) {
        dprintf(cfd, "SENTENCE ERROR\n");
        return;
    }
    if (joined[0] == '\0') {
        if (sentence->tail && sentence->tail[0] == '\0') {
            dprintf(cfd, "SENTENCE (empty)\n");
        } else {
            dprintf(cfd, "SENTENCE (blank)\n");
        }
    } else {
        dprintf(cfd, "SENTENCE %s\n", joined);
    }
    free(joined);
}

static void handle_write_begin(int cfd, const char *ss_id, const char *client_id, const char *arg);
static void handle_write_update(int cfd, const char *ss_id, const char *client_id, const char *index_token, const char *arg);
static void handle_write_commit(int cfd, const char *ss_id, const char *client_id);
static void handle_undo(int cfd, const char *ss_id, const char *client_id, const char *arg);

static void handle_write_begin(int cfd, const char *ss_id, const char *client_id, const char *arg) {
    if (!arg || *arg == '\0') {
        dprintf(cfd, "ERR WRITE missing_arguments\n");
        return;
    }

    char filename[ID_MAX];
    int sentence_index = 0;
    if (sscanf(arg, "%63s %d", filename, &sentence_index) != 2) {
        dprintf(cfd, "ERR WRITE bad_arguments\n");
        return;
    }
    if (!is_valid_name_component(filename)) {
        dprintf(cfd, "ERR WRITE invalid_filename\n");
        return;
    }
    if (sentence_index <= 0) {
        dprintf(cfd, "ERR WRITE bad_sentence_index\n");
        return;
    }

    if (session_find_by_client(client_id)) {
        dprintf(cfd, "ERR WRITE already_in_progress\n");
        return;
    }

    WriteSession *locked = session_find_lock(filename, sentence_index);
    if (locked && strcmp(locked->client_id, client_id) != 0) {
        dprintf(cfd, "ERR WRITE locked %s %d\n", filename, sentence_index);
        return;
    }

    char path[PATH_MAX];
    if (storage_path_for(filename, path, sizeof(path)) < 0) {
        dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
        return;
    }

    Document doc;
    mode_t mode = 0666;
    if (document_parse_from_path(path, &doc, &mode) < 0) {
        dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
        return;
    }

    if ((size_t)sentence_index == 0) {
        document_free(&doc);
        dprintf(cfd, "ERR WRITE sentence_out_of_range\n");
        return;
    }

    size_t current_count = doc.count;
    size_t desired = (size_t)sentence_index;
    if (desired > current_count + 1) {
        document_free(&doc);
        dprintf(cfd, "ERR WRITE sentence_out_of_range\n");
        return;
    }

    if (desired == current_count + 1) {
        Sentence *prev = current_count > 0 ? &doc.items[current_count - 1] : NULL;
        Sentence *ns = document_new_sentence(&doc);
        if (!ns) {
            document_free(&doc);
            dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
            return;
        }
        if (prev && (!prev->tail || prev->tail[0] == '\0')) {
            if (sentence_set_tail(prev, "\n", 1) < 0) {
                document_free(&doc);
                dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
                return;
            }
        }
    }

    WriteSession *session = session_allocate();
    if (!session) {
        document_free(&doc);
        dprintf(cfd, "ERR WRITE too_many_sessions\n");
        return;
    }

    session->doc = doc;
    strncpy(session->client_id, client_id, sizeof(session->client_id) - 1);
    session->client_id[sizeof(session->client_id) - 1] = '\0';
    strncpy(session->filename, filename, sizeof(session->filename) - 1);
    session->filename[sizeof(session->filename) - 1] = '\0';
    session->sentence_index = sentence_index;
    session->original_mode = mode;

    Sentence *target = session_target_sentence(session);
    if (!target) {
        dprintf(cfd, "ERR WRITE internal_error\n");
        session_release(session);
        return;
    }

    dprintf(cfd, "ACK %s WRITE READY %s %d %zu\n", ss_id, filename, sentence_index, target->words.count);
    send_sentence_snapshot(cfd, target);
}

static void handle_write_update(int cfd, const char *ss_id, const char *client_id, const char *index_token, const char *arg) {
    if (!index_token || !is_number_string(index_token)) {
        dprintf(cfd, "ERR WRITE bad_word_index\n");
        return;
    }
    if (!arg || *arg == '\0') {
        dprintf(cfd, "ERR WRITE missing_content\n");
        return;
    }

    long idx_long = strtol(index_token, NULL, 10);
    if (idx_long <= 0 || idx_long > INT_MAX) {
        dprintf(cfd, "ERR WRITE bad_word_index\n");
        return;
    }
    int word_index = (int)idx_long;

    WriteSession *session = session_find_by_client(client_id);
    if (!session) {
        dprintf(cfd, "ERR WRITE no_active_session\n");
        return;
    }

    Sentence *target = session_target_sentence(session);
    if (!target) {
        dprintf(cfd, "ERR WRITE internal_error\n");
        session_release(session);
        return;
    }

    if ((size_t)(word_index - 1) < target->words.count) {
        if (word_array_replace(&target->words, (size_t)(word_index - 1), arg) < 0) {
            dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
            return;
        }
    } else if ((size_t)word_index == target->words.count + 1) {
        if (word_array_append_string(&target->words, arg) < 0) {
            dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
            return;
        }
    } else {
        dprintf(cfd, "ERR WRITE index_out_of_bounds\n");
        return;
    }

    dprintf(cfd, "ACK %s WRITE UPDATED %s %d %d\n", ss_id, session->filename, session->sentence_index, word_index);
    send_sentence_snapshot(cfd, target);
}

static void handle_write_commit(int cfd, const char *ss_id, const char *client_id) {
    WriteSession *session = session_find_by_client(client_id);
    if (!session) {
        dprintf(cfd, "ERR WRITE no_active_session\n");
        return;
    }

    Sentence *target = session_target_sentence(session);
    if (!target) {
        dprintf(cfd, "ERR WRITE internal_error\n");
        session_release(session);
        return;
    }

    if (target->words.count == 0 && (!target->tail || target->tail[0] == '\0')) {
        if (sentence_set_tail(target, "\n", 1) < 0) {
            dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
            session_release(session);
            return;
        }
    }

    char path[PATH_MAX];
    if (storage_path_for(session->filename, path, sizeof(path)) < 0) {
        dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
        session_release(session);
        return;
    }

    char undo_path[PATH_MAX];
    if (undo_path_for(session->filename, undo_path, sizeof(undo_path)) < 0) {
        dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
        session_release(session);
        return;
    }

    if (copy_file_to_path(path, undo_path) < 0) {
        if (errno != ENOENT) {
            dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
            session_release(session);
            return;
        }
    }

    if (write_document_to_path(&session->doc, path, session->original_mode) < 0) {
        dprintf(cfd, "ERR WRITE %s\n", strerror(errno));
        session_release(session);
        return;
    }

    // update info metadata: last modified and size
    char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
    char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
    load_info_fields(session->filename,
                     owner, sizeof(owner),
                     access_str, sizeof(access_str),
                     created, sizeof(created),
                     lastmod, sizeof(lastmod),
                     last_access_ts, sizeof(last_access_ts),
                     last_access_user, sizeof(last_access_user));
    char now[64]; format_time_now(now, sizeof(now));
    write_info_txt(session->filename,
                   owner[0]?owner:NULL,
                   access_str,
                   created[0]?created:NULL,
                   now,
                   now,
                   client_id);

    dprintf(cfd, "ACK %s WRITE COMMIT %s %d\n", ss_id, session->filename, session->sentence_index);
    send_sentence_snapshot(cfd, target);
    session_release(session);
}

static void handle_undo(int cfd, const char *ss_id, const char *client_id, const char *arg) {
    (void)client_id;
    char filename[ID_MAX];
    if (!arg || sscanf(arg, "%63s", filename) != 1) {
        dprintf(cfd, "ERR UNDO missing_filename\n");
        return;
    }
    if (!is_valid_name_component(filename)) {
        dprintf(cfd, "ERR UNDO invalid_filename\n");
        return;
    }

    if (file_has_active_session(filename)) {
        dprintf(cfd, "ERR UNDO write_in_progress\n");
        return;
    }

    char current_path[PATH_MAX];
    if (storage_path_for(filename, current_path, sizeof(current_path)) < 0) {
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }
    struct stat st;
    if (stat(current_path, &st) < 0) {
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }

    char undo_path[PATH_MAX];
    if (undo_path_for(filename, undo_path, sizeof(undo_path)) < 0) {
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }
    if (stat(undo_path, &st) < 0) {
        if (errno == ENOENT) {
            dprintf(cfd, "ERR UNDO no_history\n");
        } else {
            dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        }
        return;
    }

    char swap_path[PATH_MAX];
    if (swap_path_for(filename, swap_path, sizeof(swap_path)) < 0) {
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }

    unlink(swap_path);
    if (rename(current_path, swap_path) < 0) {
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }
    if (rename(undo_path, current_path) < 0) {
        int saved = errno;
        rename(swap_path, current_path);
        errno = saved;
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }
    if (rename(swap_path, undo_path) < 0) {
        int saved = errno;
        rename(current_path, undo_path);
        rename(swap_path, current_path);
        errno = saved;
        dprintf(cfd, "ERR UNDO %s\n", strerror(errno));
        return;
    }

    dprintf(cfd, "ACK %s UNDO OK %s\n", ss_id, filename);
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
                                    printf("[SS %s] created file %s for client %s\n", ss_id, fname, client_id);
                                    fflush(stdout);
                                    dprintf(cfd, "ACK %s CREATE OK %s\n", ss_id, fname);
                                } else {
                                    int err = errno;
                                    fprintf(stderr, "[SS %s] failed to create file %s for client %s: %s\n",
                                            ss_id, fname, client_id, strerror(err));
                                    fflush(stderr);
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
                    } else if (strcmp(verb_upper, "EXEC") == 0) {
                        if (!arg || arg[0] == '\0') {
                            dprintf(cfd, "ERR EXEC missing_filename\n");
                        } else if (!is_valid_name_component(arg)) {
                            dprintf(cfd, "ERR EXEC invalid_filename\n");
                        } else {
                            int fd = open_storage_file_ro(arg);
                            if (fd < 0) {
                                int err = errno;
                                fprintf(stderr, "[SS %s] failed to open file %s for EXEC by %s: %s\n",
                                        ss_id, arg, client_id, strerror(err));
                                fflush(stderr);
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
                                    printf("[SS %s] sent EXEC payload %s to %s\n", ss_id, arg, client_id); fflush(stdout);
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
                            char owner[ID_MAX]={0}, access_str[256]={0}, created[64]={0}, lastmod[64]={0};
                            char last_access_ts[64]={0}, last_access_user[ID_MAX]={0};
                            load_info_fields(fname,
                                             owner, sizeof(owner),
                                             access_str, sizeof(access_str),
                                             created, sizeof(created),
                                             lastmod, sizeof(lastmod),
                                             last_access_ts, sizeof(last_access_ts),
                                             last_access_user, sizeof(last_access_user));
                            if (strstr(access_str, user) == NULL) {
                                char new_access[512]; if (access_str[0]) snprintf(new_access, sizeof(new_access), "%s, %s (%s)", access_str, user, (strcmp(flag, "-W")==0)?"RW":"R"); else snprintf(new_access, sizeof(new_access), "%s (%s)", user, (strcmp(flag, "-W")==0)?"RW":"R");
                                strncpy(access_str, new_access, sizeof(access_str)-1); access_str[sizeof(access_str)-1]='\0';
                            }
                            write_info_txt(fname,
                                           owner[0]?owner:NULL,
                                           access_str,
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
