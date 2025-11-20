// NamedServer.c (Step-2)
//
// Protocol supported now:
//   StorageServer -> NM:
//     REGISTER <ss_id> <client_port>
//   Client -> NM:
//     HELLOCLIENT <client_id>
//     CLIENT <client_id> <payload>
//   where <payload> is one of:
//     VIEW | VIEW -a | VIEW -l | VIEW -al
//     CREATE <filename>
//     READ <filename>
//     WRITE <filename> <sentence_number>   (followed by many lines, ends with ETIRW)
//     UNDO <filename>
//     DELETE <filename>
//     STREAM <filename>
//     EXEC <filename>
//     LIST
//     ADDACCESS -R <filename> <username>
//     ADDACCESS -W <filename> <username>
//     REMACCESS  <filename> <username>
//
// StorageServer contract (unchanged from Step-1):
//   Accepts ONE line then replies with "ACK <ss_id>\n" and closes.
//   To stay compatible, for multi-line WRITE we connect ONCE PER LINE:
//   "HELLO <client_id> <line-as-typed>"
//
// Build: gcc -pthread -o NamedServer NamedServer.c
// Run:   ./NamedServer

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define NM_PORT 5000
#define MAX_SS  512
#define ID_MAX  64
#define IP_MAX  INET_ADDRSTRLEN
#define FNAME_MAX 256
#define LINE_MAX 4096

#define FILE_INITIAL_CAPACITY 256
#define FILE_MAP_INIT_CAP (FILE_INITIAL_CAPACITY*2)
#define FILE_CACHE_SIZE 64

// ----------------------- string list -----------------------
typedef struct {
    char **v;
    int n, cap;
} StrList;

static void sl_init(StrList *s){ s->v=NULL; s->n=0; s->cap=0; }
static void sl_free(StrList *s){ for(int i=0;i<s->n;i++) free(s->v[i]); free(s->v); s->v=NULL; s->n=s->cap=0; }
static void sl_reserve(StrList *s, int need){ if(need<=s->cap) return; int nc = s->cap? s->cap*2:8; if(nc<need) nc=need; s->v=realloc(s->v, nc*sizeof(char*)); s->cap=nc; }
static int  sl_index_of(StrList *s, const char *x){ for(int i=0;i<s->n;i++) if(strcmp(s->v[i],x)==0) return i; return -1; }
static bool sl_contains(StrList *s, const char *x){ return sl_index_of(s,x)>=0; }
static void sl_add_unique(StrList *s, const char *x){
    if(sl_contains(s,x)) return; sl_reserve(s, s->n+1);
    s->v[s->n++] = strdup(x);
}
static void sl_remove(StrList *s, const char *x){
    int i=sl_index_of(s,x); if(i<0) return;
    free(s->v[i]);
    for(int j=i+1;j<s->n;j++) s->v[j-1]=s->v[j];
    s->n--;
}

// ----------------------- file metadata -----------------------
typedef struct {
    char filename[FNAME_MAX];
    char ss_id[ID_MAX];
    char owner[ID_MAX];
    StrList rd, wr, ex;
} FILE_META_DATA;

typedef struct {
    FILE_META_DATA *v;
    int n, cap;
} FileVec;

static void fv_init(FileVec *fv){ fv->v=NULL; fv->n=0; fv->cap=0; }
static void fv_free(FileVec *fv){
    for(int i=0;i<fv->n;i++){ sl_free(&fv->v[i].rd); sl_free(&fv->v[i].wr); sl_free(&fv->v[i].ex); }
    free(fv->v); fv->v=NULL; fv->n=fv->cap=0;
}
static void fv_reserve(FileVec *fv, int need){
    if(need<=fv->cap) return;
    int nc = fv->cap ? fv->cap*2 : FILE_INITIAL_CAPACITY;
    if(nc<need) nc=need;
    fv->v=realloc(fv->v, nc*sizeof(FILE_META_DATA));
    fv->cap=nc;
}
static FILE_META_DATA* fv_add(FileVec *fv){
    fv_reserve(fv, fv->n+1); FILE_META_DATA *m=&fv->v[fv->n++]; memset(m,0,sizeof(*m));
    sl_init(&m->rd); sl_init(&m->wr); sl_init(&m->ex); return m;
}
typedef struct {
    char *key;
    int index;
    unsigned char state; // 0=empty,1=used,2=tombstone
} FileHashEntry;

typedef struct {
    FileHashEntry *entries;
    int capacity;
    int size;
} FileHashMap;

typedef struct {
    char filename[FNAME_MAX];
    int index;
    unsigned int age;
    bool valid;
} FileCacheEntry;

typedef struct {
    FileCacheEntry entries[FILE_CACHE_SIZE];
    unsigned int tick;
} FileCache;

static FileHashMap FILE_MAP;
static FileCache FILE_CACHE;
static int g_file_cap = FILE_INITIAL_CAPACITY;

static uint64_t hash_string(const char *s){
    uint64_t h = 1469598103934665603ULL; // FNV-1a
    while(*s){
        h ^= (unsigned char)(*s++);
        h *= 1099511628211ULL;
    }
    return h;
}

static void file_map_init(FileHashMap *map){
    map->capacity = FILE_MAP_INIT_CAP;
    map->size = 0;
    map->entries = calloc(map->capacity, sizeof(FileHashEntry));
}

static void file_cache_init(FileCache *cache){ memset(cache, 0, sizeof(*cache)); }

static int file_map_probe(FileHashMap *map, const char *key, bool *found){
    if(map->capacity == 0){ *found = false; return -1; }
    uint64_t base = hash_string(key);
    int first_tomb = -1;
    for(int i=0;i<map->capacity;i++){
        int idx = (int)((base + (uint64_t)i) % (uint64_t)map->capacity);
        FileHashEntry *e = &map->entries[idx];
        if(e->state == 0){
            *found = false;
            return (first_tomb>=0)? first_tomb : idx;
        }
        if(e->state == 1 && strcmp(e->key, key)==0){
            *found = true;
            return idx;
        }
        if(e->state == 2 && first_tomb < 0) first_tomb = idx;
    }
    *found = false;
    return (first_tomb>=0)? first_tomb : -1;
}

static void file_map_put_entry(FileHashMap *map, char *key, int index){
    bool found=false;
    int slot = file_map_probe(map, key, &found);
    if(slot<0) return;
    if(found){
        map->entries[slot].index = index;
        return;
    }
    map->entries[slot].key = key;
    map->entries[slot].index = index;
    map->entries[slot].state = 1;
    map->size++;
}

static void file_map_rehash(FileHashMap *map, int new_cap){
    FileHashEntry *old_entries = map->entries;
    int old_cap = map->capacity;
    map->entries = calloc(new_cap, sizeof(FileHashEntry));
    map->capacity = new_cap;
    map->size = 0;
    for(int i=0;i<old_cap;i++){
        if(old_entries[i].state == 1){
            file_map_put_entry(map, old_entries[i].key, old_entries[i].index);
        }
    }
    free(old_entries);
}

static void file_map_set(FileHashMap *map, const char *key, int index){
    if(map->capacity == 0) file_map_init(map);
    if((map->size + 1) * 100 / map->capacity > 70){
        file_map_rehash(map, map->capacity * 2);
    }
    bool found=false;
    int slot = file_map_probe(map, key, &found);
    if(slot<0) return;
    if(found){
        map->entries[slot].index = index;
        return;
    }
    map->entries[slot].key = strdup(key);
    map->entries[slot].index = index;
    map->entries[slot].state = 1;
    map->size++;
}

static int file_map_get(FileHashMap *map, const char *key){
    if(map->capacity == 0) return -1;
    bool found=false;
    int slot = file_map_probe(map, key, &found);
    if(found && slot>=0) return map->entries[slot].index;
    return -1;
}

static void file_map_update(FileHashMap *map, const char *key, int index){
    if(map->capacity == 0) return;
    bool found=false;
    int slot = file_map_probe(map, key, &found);
    if(found && slot>=0) map->entries[slot].index = index;
}

static void file_map_remove(FileHashMap *map, const char *key){
    if(map->capacity == 0) return;
    bool found=false;
    int slot = file_map_probe(map, key, &found);
    if(!found || slot<0) return;
    free(map->entries[slot].key);
    map->entries[slot].key = NULL;
    map->entries[slot].index = -1;
    map->entries[slot].state = 2;
    if(map->size>0) map->size--;
}

static void file_map_free(FileHashMap *map){
    if(!map->entries) return;
    for(int i=0;i<map->capacity;i++) if(map->entries[i].state == 1) free(map->entries[i].key);
    free(map->entries);
    map->entries=NULL;
    map->capacity=0;
    map->size=0;
}

static bool file_cache_lookup(FileCache *cache, const char *fname, int *out_index){
    for(int i=0;i<FILE_CACHE_SIZE;i++){
        FileCacheEntry *e=&cache->entries[i];
        if(e->valid && strcmp(e->filename,fname)==0){
            e->age = ++cache->tick;
            *out_index = e->index;
            return true;
        }
    }
    return false;
}

static void file_cache_insert(FileCache *cache, const char *fname, int index){
    FileCacheEntry *slot=NULL;
    FileCacheEntry *oldest=NULL;
    for(int i=0;i<FILE_CACHE_SIZE;i++){
        FileCacheEntry *e=&cache->entries[i];
        if(!e->valid){ slot=e; break; }
        if(!oldest || e->age < oldest->age) oldest = e;
    }
    if(!slot) slot = oldest;
    slot->valid = true;
    slot->index = index;
    slot->age = ++cache->tick;
    strncpy(slot->filename, fname, FNAME_MAX-1);
    slot->filename[FNAME_MAX-1]='\0';
}

static void file_cache_invalidate(FileCache *cache, const char *fname){
    for(int i=0;i<FILE_CACHE_SIZE;i++){
        FileCacheEntry *e=&cache->entries[i];
        if(e->valid && strcmp(e->filename,fname)==0){
            e->valid=false;
            return;
        }
    }
}

static void file_cache_update(FileCache *cache, const char *fname, int index){
    for(int i=0;i<FILE_CACHE_SIZE;i++){
        FileCacheEntry *e=&cache->entries[i];
        if(e->valid && strcmp(e->filename,fname)==0){
            e->index = index;
            e->age = ++cache->tick;
            return;
        }
    }
}

static int fv_find(FileVec *fv, const char *fname){
    int idx=-1;
    if(file_cache_lookup(&FILE_CACHE, fname, &idx)){
        if(idx>=0 && idx<fv->n && strcmp(fv->v[idx].filename,fname)==0) return idx;
        file_cache_invalidate(&FILE_CACHE, fname);
    }
    idx = file_map_get(&FILE_MAP, fname);
    if(idx>=0 && idx<fv->n && strcmp(fv->v[idx].filename,fname)==0){
        file_cache_insert(&FILE_CACHE, fname, idx);
        return idx;
    }
    if(idx>=0) file_map_remove(&FILE_MAP, fname);
    return -1;
}

static void fv_remove_at(FileVec *fv, int idx){
    if(idx<0 || idx>=fv->n) return;
    FILE_META_DATA removed = fv->v[idx];
    file_cache_invalidate(&FILE_CACHE, removed.filename);
    file_map_remove(&FILE_MAP, removed.filename);

    int last = fv->n - 1;
    if(idx != last){
        FILE_META_DATA moved = fv->v[last];
        fv->v[idx] = moved;
        file_map_update(&FILE_MAP, fv->v[idx].filename, idx);
        file_cache_update(&FILE_CACHE, fv->v[idx].filename, idx);
    }
    fv->n--;
    sl_free(&removed.rd);
    sl_free(&removed.wr);
    sl_free(&removed.ex);
}

// ----------------------- storage server registry + heap -----------------------
typedef struct {
    char id[ID_MAX];
    char ip[IP_MAX];
    int  client_port;
    bool in_use;
    int  file_count;   // for load balancing
    int  heap_index;   // -1 if not in heap
} SSInfo;

static SSInfo g_ss[MAX_SS];
static int g_ss_count = 0;

static int g_heap[MAX_SS];
static int g_heap_sz = 0;

static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

static int ss_find_index(const char *id){
    for(int i=0;i<g_ss_count;i++) if(g_ss[i].in_use && strcmp(g_ss[i].id,id)==0) return i;
    return -1;
}

static int heap_cmp(int a, int b){
    if(g_ss[a].file_count != g_ss[b].file_count) return g_ss[a].file_count - g_ss[b].file_count;
    return strcmp(g_ss[a].id, g_ss[b].id);
}
static void heap_swap(int i, int j){
    int ai = g_heap[i], aj = g_heap[j];
    g_heap[i]=aj; g_heap[j]=ai;
    g_ss[aj].heap_index=i; g_ss[ai].heap_index=j;
}
static void heap_up(int i){
    while(i>0){
        int p=(i-1)/2;
        if(heap_cmp(g_heap[i], g_heap[p])>=0) break;
        heap_swap(i,p); i=p;
    }
}
static void heap_dn(int i){
    for(;;){
        int l=2*i+1, r=l+1, m=i;
        if(l<g_heap_sz && heap_cmp(g_heap[l], g_heap[m])<0) m=l;
        if(r<g_heap_sz && heap_cmp(g_heap[r], g_heap[m])<0) m=r;
        if(m==i) break;
        heap_swap(i,m); i=m;
    }
}
static void heap_insert(int ss_idx){
    if(g_ss[ss_idx].heap_index!=-1) return;
    int i=g_heap_sz++;
    g_heap[i]=ss_idx;
    g_ss[ss_idx].heap_index=i;
    heap_up(i);
}
static void heap_update(int ss_idx){
    int i = g_ss[ss_idx].heap_index;
    if(i<0) return;
    heap_up(i); heap_dn(i);
}
static int heap_top(){
    if(g_heap_sz==0) return -1;
    return g_heap[0];
}

// ----------------------- global metadata -----------------------
static FileVec ALL_FILES;
static StrList USER_LIST;

// Ensure metadata storage grows geometrically as the file count rises.
static void ensure_file_capacity(void){
    if(ALL_FILES.n < g_file_cap) return;
    int new_cap = g_file_cap * 2;
    fv_reserve(&ALL_FILES, new_cap);
    if(FILE_MAP.capacity == 0) file_map_init(&FILE_MAP);
    if(FILE_MAP.capacity < new_cap * 2){
        file_map_rehash(&FILE_MAP, new_cap * 2);
    }
    g_file_cap = new_cap;
}

// ----------------------- socket helpers -----------------------
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
    if (listen(fd, 256) < 0) { perror("listen"); exit(1); }
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
static void get_peer_ip(int fd, char out_ip[IP_MAX]) {
    struct sockaddr_in addr; socklen_t len = sizeof(addr);
    if (getpeername(fd, (struct sockaddr*)&addr, &len) == 0) {
        inet_ntop(AF_INET, &addr.sin_addr, out_ip, IP_MAX);
    } else { strncpy(out_ip, "127.0.0.1", IP_MAX); }
}
static int connect_to_host(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { return -1; }
    struct sockaddr_in a; memset(&a,0,sizeof(a));
    a.sin_family=AF_INET; a.sin_port=htons(port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) < 0) { close(fd); return -1; }
    return fd;
}
static void send_line(int fd, const char *fmt, ...) {
    char buf[LINE_MAX];
    va_list ap; va_start(ap,fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    size_t len = strlen(buf);
    // ensure newline
    if(len==0 || buf[len-1] != '\n'){ buf[len++] = '\n'; buf[len]=0; }
    send(fd, buf, len, MSG_NOSIGNAL);
}

// ----------------------- permissions helpers -----------------------
static bool has_read(const FILE_META_DATA *m, const char *user){
    return strcmp(m->owner,user)==0 || sl_contains((StrList*)&m->rd, user);
}
static bool has_write(const FILE_META_DATA *m, const char *user){
    return strcmp(m->owner,user)==0 || sl_contains((StrList*)&m->wr, user);
}
static bool has_exec(const FILE_META_DATA *m, const char *user){
    return strcmp(m->owner,user)==0 || sl_contains((StrList*)&m->ex, user);
}

// ----------------------- registry actions -----------------------
static void register_ss(const char *id, int client_port, const char *peer_ip) {
    pthread_mutex_lock(&g_mtx);
    int idx = ss_find_index(id);
    if(idx>=0){
        strncpy(g_ss[idx].ip, peer_ip, IP_MAX-1);
        g_ss[idx].client_port = client_port;
        if(g_ss[idx].heap_index==-1) heap_insert(idx);
        pthread_mutex_unlock(&g_mtx);
        printf("[NM] Storage server registered SSid: %s at port %s:%d (no_of_files=%d)\n",
               id, peer_ip, client_port, g_ss[idx].file_count);
        fflush(stdout);
        return;
    }
    if (g_ss_count >= MAX_SS) {
        pthread_mutex_unlock(&g_mtx);
        fprintf(stderr, "[NM] ERROR could not register SSid: %s as registry is full;\n", id);
        return;
    }
    SSInfo *s = &g_ss[g_ss_count];
    memset(s,0,sizeof(*s));
    strncpy(s->id, id, ID_MAX-1);
    strncpy(s->ip, peer_ip, IP_MAX-1);
    s->client_port = client_port;
    s->in_use = true;
    s->file_count = 0;
    s->heap_index = -1;
    heap_insert(g_ss_count);
    g_ss_count++;
    pthread_mutex_unlock(&g_mtx);
    printf("[NM] Storage server registered SSid: %s at port %s:%d (no_of_files=%d)\n",
           id, peer_ip, client_port, g_ss_count);
    fflush(stdout);
}

static int pick_least_loaded_ss() {
    pthread_mutex_lock(&g_mtx);
    int idx = heap_top();
    pthread_mutex_unlock(&g_mtx);
    return idx;
}

static void ss_increase_load(const char *ss_id){
    pthread_mutex_lock(&g_mtx);
    int idx = ss_find_index(ss_id);
    if(idx>=0){ g_ss[idx].file_count++; heap_update(idx); }
    pthread_mutex_unlock(&g_mtx);
}
static void ss_decrease_load(const char *ss_id){
    pthread_mutex_lock(&g_mtx);
    int idx = ss_find_index(ss_id);
    if(idx>=0 && g_ss[idx].file_count>0){ g_ss[idx].file_count--; heap_update(idx); }
    pthread_mutex_unlock(&g_mtx);
}

// Send ONE logical line to SS as: "HELLO <client_id> <payload>"
static void send_one_line_to_ss(const char *ss_id, const char *client_id, const char *payload, int client_fd_for_echo){
    pthread_mutex_lock(&g_mtx);
    int sidx = ss_find_index(ss_id);
    SSInfo ss; memset(&ss,0,sizeof(ss));
    if(sidx>=0) ss = g_ss[sidx];
    pthread_mutex_unlock(&g_mtx);
    if(sidx<0){ send_line(client_fd_for_echo, "ERROR Storage Server Not Fount %s", ss_id); return; }

    int ssfd = connect_to_host(ss.ip, ss.client_port);
    if(ssfd<0){ send_line(client_fd_for_echo, "ERROR Connection failed to Storage Server %s", ss_id); return; }

    // forward single line
    send_line(ssfd, "HELLO %s %s", client_id, payload);

    // pipe back any quick reply from SS
    char buf[LINE_MAX]; ssize_t n;
    while((n=recv(ssfd, buf, sizeof(buf)-1, 0))>0){
        buf[n]='\0';
        send(client_fd_for_echo, buf, n, MSG_NOSIGNAL);
    }
    close(ssfd);
}

// ----------------------- view/list helpers -----------------------
static void print_file_brief(int fd, const FILE_META_DATA *m){
    send_line(fd, "%s", m->filename);
}
static void print_file_long(int fd, const FILE_META_DATA *m){
    send_line(fd, "file: %s", m->filename);
    send_line(fd, "  owner: %s", m->owner);
    send_line(fd, "  storage_server: %s", m->ss_id);
    // print lists
    {
        char tmp[LINE_MAX]; tmp[0]='\0';
        strcat(tmp, "  read: ");
        for(int i=0;i<m->rd.n;i++){ strcat(tmp, (i?",":"")); strcat(tmp, m->rd.v[i]); }
        send_line(fd, "%s", tmp);
    }
    {
        char tmp[LINE_MAX]; tmp[0]='\0';
        strcat(tmp, "  write: ");
        for(int i=0;i<m->wr.n;i++){ strcat(tmp, (i?",":"")); strcat(tmp, m->wr.v[i]); }
        send_line(fd, "%s", tmp);
    }
    {
        char tmp[LINE_MAX]; tmp[0]='\0';
        strcat(tmp, "  exec: ");
        for(int i=0;i<m->ex.n;i++){ strcat(tmp, (i?",":"")); strcat(tmp, m->ex.v[i]); }
        send_line(fd, "%s", tmp);
    }
}

// ----------------------- command handling -----------------------
static void handle_view(int cfd, const char *client, const char *flag){
    bool all = false;
    bool longlist = false;

    if(flag){
        size_t flag_len = strlen(flag);
        if(flag_len >= 8){
            send_line(cfd, "ERROR incorrect flags for VIEW");
            return;
        }
        if(strcmp(flag,"-a")==0){
            all = true;
        } else if(strcmp(flag,"-l")==0){
            longlist = true;
        } else if(strcmp(flag,"-al")==0 || strcmp(flag,"-la")==0){
            all = true;
            longlist = true;
        } else {
            send_line(cfd, "ERROR incorrect flags for VIEW");
            return;
        }
    }

    pthread_mutex_lock(&g_mtx);
    if(!flag || longlist) {
        send_line(cfd, "VIEW results:");
    }
    for(int i=0;i<ALL_FILES.n;i++){
        FILE_META_DATA *m=&ALL_FILES.v[i];
        bool visible = all || has_read(m,client) || has_write(m,client) || has_exec(m,client) || strcmp(m->owner,client)==0;
        if(!visible) continue;
        if(longlist) print_file_long(cfd, m);
        else         print_file_brief(cfd, m);
    }
    pthread_mutex_unlock(&g_mtx);
}

static void handle_list_users(int cfd){
    pthread_mutex_lock(&g_mtx);
    send_line(cfd, "USERS (%d):", USER_LIST.n);
    for(int i=0;i<USER_LIST.n;i++) send_line(cfd, "  %s", USER_LIST.v[i]);
    pthread_mutex_unlock(&g_mtx);
}

static void handle_create(int cfd, const char *client, const char *fname){
    if(!fname || !*fname){ send_line(cfd,"ERROR Create failed: Check File Name"); return; }

    pthread_mutex_lock(&g_mtx);
    if(fv_find(&ALL_FILES, fname)>=0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd, "ERROR Create failed as file already exists");
        return;
    }
    int sidx = heap_top();
    if(sidx<0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd, "ERROR Error no Storage server exists");
        return;
    }
    // Grow metadata tables if needed, then create entry
    ensure_file_capacity();
    FILE_META_DATA *m = fv_add(&ALL_FILES);
    strncpy(m->filename, fname, FNAME_MAX-1);
    strncpy(m->ss_id,    g_ss[sidx].id, ID_MAX-1);
    strncpy(m->owner,    client, ID_MAX-1);
    sl_add_unique(&m->rd, client);
    sl_add_unique(&m->wr, client);
    sl_add_unique(&m->ex, client);
    int new_index = ALL_FILES.n - 1;
    file_map_set(&FILE_MAP, m->filename, new_index);
    file_cache_insert(&FILE_CACHE, m->filename, new_index);
    // Update heap load
    g_ss[sidx].file_count++;
    heap_update(sidx);
    SSInfo ss = g_ss[sidx];
    pthread_mutex_unlock(&g_mtx);

    send_line(cfd, "Create successful CREATED %s on SSId: %s:%d (ss=%s)", fname, ss.ip, ss.client_port, ss.id);

    // notify SS (single line)
    char payload[LINE_MAX];
    snprintf(payload,sizeof(payload), "CREATE %s %s", fname, client);
    send_one_line_to_ss(ss.id, client, payload, cfd);
}

static void handle_addaccess(int cfd, const char *client, const char *flag, const char *fname, const char *user){
    if(!flag || !*flag || !fname || !*fname || !user || !*user){
        send_line(cfd, "ERROR BAD_ADDACCESS");
        return;
    }
    if(strcmp(flag, "-R")!=0 && strcmp(flag, "-W")!=0){
        send_line(cfd, "ERROR incorrect Flags");
        return;
    }

    pthread_mutex_lock(&g_mtx);
    if(!sl_contains(&USER_LIST, user)){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR User Doesnt Exist"); return; }
    int i = fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR File Doesnt Exist"); return; }
    FILE_META_DATA *m=&ALL_FILES.v[i];
    if(strcmp(m->owner, client)!=0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR Not the Owner"); return; }

    if(strcmp(flag,"-R")==0){
        if(sl_contains(&m->rd, user)){
            pthread_mutex_unlock(&g_mtx);
            send_line(cfd, "ERROR User already has Read access");
            return;
        }
        sl_add_unique(&m->rd, user);
    }
    else {
        if(sl_contains(&m->wr, user)){
            pthread_mutex_unlock(&g_mtx);
            send_line(cfd, "ERROR User already has Write access");
            return;
        }
        sl_add_unique(&m->wr, user);
    }

    // notify SS to persist access change
    char payload[LINE_MAX]; snprintf(payload,sizeof(payload), "ADDACCESS %s %s %s", flag, fname, user);
    char ss_id[ID_MAX]; strncpy(ss_id, m->ss_id, ID_MAX-1); ss_id[ID_MAX-1]='\0';
    pthread_mutex_unlock(&g_mtx);
    send_line(cfd, "OK Access Added succesfully %s %s %s", flag, fname, user);
    send_one_line_to_ss(ss_id, client, payload, cfd);
}

static void handle_remaccess(int cfd, const char *client, const char *fname, const char *user){
    if(!fname || !user){ send_line(cfd,"ERROR BAD_REMACCESS"); return; }
    pthread_mutex_lock(&g_mtx);
    if(!sl_contains(&USER_LIST, user)){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR User Doesnt Exist"); return; }
    int i = fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR NO_SUCH_FILE"); return; }
    FILE_META_DATA *m=&ALL_FILES.v[i];
    if(strcmp(m->owner, client)!=0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR NOT_OWNER"); return; }
    if(strcmp(user, m->owner)==0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd, "Cannot remove your own access");
        return;
    }
    sl_remove(&m->rd, user);
    sl_remove(&m->wr, user);
    char payload[LINE_MAX]; snprintf(payload,sizeof(payload), "REMACCESS %s %s", fname, user);
    char ss_id[ID_MAX]; strncpy(ss_id, m->ss_id, ID_MAX-1); ss_id[ID_MAX-1]='\0';
    pthread_mutex_unlock(&g_mtx);
    send_line(cfd, "OK REMACCESS %s %s", fname, user);
    send_one_line_to_ss(ss_id, client, payload, cfd);
}

static void handle_delete(int cfd, const char *client, const char *fname){
    // printf("Going to delete\n");
    pthread_mutex_lock(&g_mtx);
    int i = fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR File Doesnot exist"); return; }
    FILE_META_DATA m = ALL_FILES.v[i]; // copy
    if(strcmp(m.owner, client)!=0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd,"ERROR Not the Owner");
        return;
    }
    fv_remove_at(&ALL_FILES, i);
    pthread_mutex_unlock(&g_mtx);

    ss_decrease_load(m.ss_id);

    send_line(cfd, "OK Deleted Successfully %s", fname);
    
    // notify SS
    char payload[LINE_MAX]; snprintf(payload,sizeof(payload),"DELETE %s", fname);
    send_one_line_to_ss(m.ss_id, client, payload, cfd);
}
static void handle_read(int cfd, const char *client, const char *fname, const char *sentence_opt){
    if(!fname || !*fname){ send_line(cfd,"Read was unsuccessful"); return; }

    pthread_mutex_lock(&g_mtx);
    int i = fv_find(&ALL_FILES, fname);
    if(i < 0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd,"ERROR File Doesnot Exist");
        return;
    }
    FILE_META_DATA *m = &ALL_FILES.v[i];
    bool ok = has_read(m, client);
    int sidx = ss_find_index(m->ss_id);
    SSInfo ss_ref;
    if(!ok){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd,"ERROR Permission Denied");
        return;
    }
    if(sidx < 0){
        pthread_mutex_unlock(&g_mtx);
        send_line(cfd,"ERROR Storage Server Not Found %s", m->ss_id);
        return;
    }
    ss_ref = g_ss[sidx];
    pthread_mutex_unlock(&g_mtx);

    const char *sent_tok = (sentence_opt && *sentence_opt) ? sentence_opt : "-";
    send_line(cfd, "DIRECT READ %s %s %s %s %d", fname, sent_tok, ss_ref.id, ss_ref.ip, ss_ref.client_port);
}

static void handle_stream_or_exec(int cfd, const char *client, const char *verb, const char *fname){
    pthread_mutex_lock(&g_mtx);
    int i=fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR NO_SUCH_FILE"); return; }
    FILE_META_DATA *m=&ALL_FILES.v[i];
    bool ok = has_read(m, client); // per your spec for EXEC too
    SSInfo ss_ref;
    int sidx = ss_find_index(m->ss_id);
    if(!ok){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR PERMISSION_DENIED"); return; }
    if(sidx<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR STORAGE_SERVER_NOT_FOUND %s", m->ss_id); return; }
    ss_ref = g_ss[sidx];
    pthread_mutex_unlock(&g_mtx);

    if(strcmp(verb, "STREAM")==0){
        send_line(cfd, "DIRECT STREAM %s - %s %s %d", fname, ss_ref.id, ss_ref.ip, ss_ref.client_port);
    }else{
        char payload[LINE_MAX]; snprintf(payload,sizeof(payload), "%s %s", verb, fname);
        send_one_line_to_ss(ss_ref.id, client, payload, cfd);
    }
}

static void handle_undo(int cfd, const char *client, const char *fname){
    pthread_mutex_lock(&g_mtx);
    int i=fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR NO_SUCH_FILE"); return; }
    FILE_META_DATA *m=&ALL_FILES.v[i];
    bool ok = has_write(m, client);
    SSInfo ss_ref; int sidx = ss_find_index(m->ss_id);
    if(!ok){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR PERMISSION_DENIED"); return; }
    if(sidx<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR STORAGE_SERVER_NOT_FOUND %s", m->ss_id); return; }
    ss_ref = g_ss[sidx];
    pthread_mutex_unlock(&g_mtx);

    char payload[LINE_MAX]; snprintf(payload,sizeof(payload), "UNDO %s", fname);
    send_one_line_to_ss(ss_ref.id, client, payload, cfd);
}

static void handle_write(int cfd, const char *client, const char *fname, const char *sent_str, int client_fd){
    (void)client_fd;
    if(!fname || !sent_str){ send_line(cfd,"ERROR BAD_WRITE"); return; }
    int sentence = atoi(sent_str);
    if(sentence <= 0){ send_line(cfd,"ERROR BAD_WRITE_SENTENCE"); return; }

    pthread_mutex_lock(&g_mtx);
    int i=fv_find(&ALL_FILES, fname);
    if(i<0){ pthread_mutex_unlock(&g_mtx); send_line(cfd,"ERROR NO_SUCH_FILE"); return; }
    FILE_META_DATA *m=&ALL_FILES.v[i];
    bool ok = has_write(m, client);
    int sidx = ss_find_index(m->ss_id);
    SSInfo ss_ref;
    if(sidx>=0) ss_ref = g_ss[sidx];
    char target_ss[ID_MAX]; strncpy(target_ss, m->ss_id, ID_MAX-1); target_ss[ID_MAX-1]='\0';
    pthread_mutex_unlock(&g_mtx);

    if(!ok){ send_line(cfd,"ERROR PERMISSION_DENIED"); return; }
    if(sidx<0){ send_line(cfd,"ERROR STORAGE_SERVER_NOT_FOUND %s", target_ss); return; }

    send_line(cfd, "DIRECT WRITE %s %s %s %s %d", fname, sent_str, ss_ref.id, ss_ref.ip, ss_ref.client_port);
}

// ----------------------- worker thread -----------------------
typedef struct { int fd; } WorkerArg;

static void *worker(void *arg_) {
    WorkerArg *wa=(WorkerArg*)arg_; int fd=wa->fd; free(wa);

    char first[LINE_MAX];
    ssize_t n = read_line(fd, first, sizeof(first));
    if(n<=0){ close(fd); return NULL; }

    if (strncmp(first, "REGISTER", 8) == 0) {
        char ss_id[ID_MAX]; int client_port=0;
        if (sscanf(first, "REGISTER %63s %d", ss_id, &client_port) == 2) {
            char ip[IP_MAX]; get_peer_ip(fd, ip);
            register_ss(ss_id, client_port, ip);
            send_line(fd, "OK");
        } else {
            send_line(fd, "ERROR malformed REGISTER");
        }
        close(fd); return NULL;
    }

    if (strncmp(first, "HELLOCLIENT", 11) == 0) {
        char client_id[ID_MAX]={0};
        if (sscanf(first, "HELLOCLIENT %63s", client_id) == 1) {
            pthread_mutex_lock(&g_mtx); sl_add_unique(&USER_LIST, client_id); pthread_mutex_unlock(&g_mtx);
            send_line(fd, "OK HELLOCLIENT %s", client_id);
        } else send_line(fd, "ERROR malformed HELLOCLIENT");
        close(fd); return NULL;
    }

    if (strncmp(first, "CLIENT", 6) == 0) {
        // CLIENT <id> <payload...>
        char client_id[ID_MAX]={0};
        char payload[LINE_MAX]={0};
        // parse id
        // we accept payload possibly empty
        if (sscanf(first, "CLIENT %63s %4095[^\n]", client_id, payload) >= 1) {
            if(client_id[0]=='\0'){ send_line(fd,"ERROR BAD_CLIENT"); close(fd); return NULL; }
            // ensure user exists
            pthread_mutex_lock(&g_mtx); sl_add_unique(&USER_LIST, client_id); pthread_mutex_unlock(&g_mtx);

            // Parse payload verb and args
            char verb[32]={0}; char a1[LINE_MAX]={0}; char a2[LINE_MAX]={0}; char a3[LINE_MAX]={0};
            // tokenization: verb [rest...]
            const char *p = payload;
            if (sscanf(p, "%31s %4095[^\n]", verb, a1) < 1) { send_line(fd,"ERROR EMPTY_COMMAND"); close(fd); return NULL; }

            // Uppercase verb compare but keep as typed
            // VIEW variants
            if (strcasecmp(verb,"VIEW")==0) {
                char flag[8]={0};
                if(sscanf(payload, "VIEW %7s", flag)==1) handle_view(fd, client_id, flag);
                else handle_view(fd, client_id, NULL);
                close(fd); return NULL;
            }

            if (strcasecmp(verb,"LIST")==0) { handle_list_users(fd); close(fd); return NULL; }

            if (strcasecmp(verb,"CREATE")==0) {
                char fname[FNAME_MAX]={0};
                if (sscanf(payload, "CREATE %255s", fname)==1) handle_create(fd, client_id, fname);
                else send_line(fd,"ERROR BAD_CREATE");
                close(fd); return NULL;
            }
            if (strcasecmp(verb,"READ")==0) {
                char fname[FNAME_MAX]={0}, sent[64]={0};
                int got = sscanf(payload, "READ %255s %63s", fname, sent);
                if (got == 1) {
                    handle_read(fd, client_id, fname, NULL);
                } else if (got == 2) {
                    handle_read(fd, client_id, fname, sent);
                } else {
                    send_line(fd,"ERROR BAD_READ");
                }
                close(fd); return NULL;
            }
            if (strcasecmp(verb,"ADDACCESS")==0) {
                    char args[LINE_MAX];
                strncpy(args, payload, sizeof(args));
                args[sizeof(args)-1] = '\0';
                char *save = NULL;
                char *tok = strtok_r(args, " \t\r\n", &save); // ADDACCESS
                char *flag_tok = strtok_r(NULL, " \t\r\n", &save);
                char *fname_tok = strtok_r(NULL, " \t\r\n", &save);
                char *user_tok = strtok_r(NULL, " \t\r\n", &save);
                char *extra_tok = strtok_r(NULL, " \t\r\n", &save);
                if(tok && flag_tok && fname_tok && user_tok && !extra_tok)
                    handle_addaccess(fd, client_id, flag_tok, fname_tok, user_tok);
                else send_line(fd,"ERROR BAD_ADDACCESS");
                close(fd); return NULL;
            }

            if (strcasecmp(verb,"REMACCESS")==0) {
                char fname[FNAME_MAX]={0}, user[ID_MAX]={0};
                if (sscanf(payload, "REMACCESS %255s %63s", fname, user)==2)
                    handle_remaccess(fd, client_id, fname, user);
                else send_line(fd,"ERROR BAD_REMACCESS");
                close(fd); return NULL;
            }

            if (strcasecmp(verb,"DELETE")==0) {
                char fname[FNAME_MAX]={0};
                if (sscanf(payload, "DELETE %255s", fname)==1)
                    handle_delete(fd, client_id, fname);
                else send_line(fd,"ERROR BAD_DELETE");
                close(fd); return NULL;
            }

            if (strcasecmp(verb,"UNDO")==0) {
                char fname[FNAME_MAX]={0};
                if (sscanf(payload, "UNDO %255s", fname)==1)
                    handle_undo(fd, client_id, fname);
                else send_line(fd,"ERROR BAD_UNDO");
                close(fd); return NULL;
            }

            if (strcasecmp(verb,"STREAM")==0) {
                char fname[FNAME_MAX]={0};
                if (sscanf(payload, "STREAM %255s", fname)==1)
                    handle_stream_or_exec(fd, client_id, "STREAM", fname);
                else send_line(fd,"ERROR BAD_STREAM");
                close(fd); return NULL;
            }

                    if (strcasecmp(verb,"INFO")==0) {
                        char fname[FNAME_MAX]={0};
                        if (sscanf(payload, "INFO %255s", fname)==1) {
                            pthread_mutex_lock(&g_mtx);
                            int i=fv_find(&ALL_FILES, fname);
                            if (i<0) { pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR NO_SUCH_FILE"); }
                            else {
                                FILE_META_DATA *m=&ALL_FILES.v[i];
                                bool ok = has_read(m, client_id) || has_write(m, client_id) || has_exec(m, client_id) || strcmp(m->owner, client_id)==0;
                                int sidx = ss_find_index(m->ss_id);
                                SSInfo ss_ref;
                                if(!ok){ pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR PERMISSION_DENIED"); }
                                else if (sidx<0){ pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR STORAGE_SERVER_NOT_FOUND %s", m->ss_id); }
                                else {
                                    ss_ref = g_ss[sidx];
                                    pthread_mutex_unlock(&g_mtx);
                                    char payload_info[LINE_MAX]; snprintf(payload_info,sizeof(payload_info), "INFO %s", fname);
                                    send_one_line_to_ss(ss_ref.id, client_id, payload_info, fd);
                                }
                            }
                        } else send_line(fd,"ERROR BAD_INFO");
                        close(fd); return NULL;
                    }

                    if (strcasecmp(verb,"EXEC")==0) {
                        char fname[FNAME_MAX]={0};
                        if (sscanf(payload, "EXEC %255s", fname)==1) {
                            // special handling: fetch file from SS and execute on NM
                            // perform permission check and locate SS
                            pthread_mutex_lock(&g_mtx);
                            int i=fv_find(&ALL_FILES, fname);
                            if (i<0) { pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR NO_SUCH_FILE"); }
                            else {
                                FILE_META_DATA *m=&ALL_FILES.v[i];
                                bool ok = has_read(m, client_id);
                                int sidx = ss_find_index(m->ss_id);
                                SSInfo ss_ref;
                                if(!ok) { pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR PERMISSION_DENIED"); }
                                else if (sidx<0) { pthread_mutex_unlock(&g_mtx); send_line(fd,"ERROR STORAGE_SERVER_NOT_FOUND %s", m->ss_id); }
                                else {
                                    ss_ref = g_ss[sidx];
                                    pthread_mutex_unlock(&g_mtx);
                                    // connect to SS and request file
                                    int ssfd = connect_to_host(ss_ref.ip, ss_ref.client_port);
                                    if (ssfd<0) { send_line(fd,"ERROR CONNECT_SS_FAILED %s", ss_ref.id); }
                                    else {
                                        // send HELLO to SS with EXEC request
                                        dprintf(ssfd, "HELLO %s EXEC %s\n", client_id, fname);
                                        // read header line
                                        char header[LINE_MAX]; if (read_line(ssfd, header, sizeof(header))<=0) { close(ssfd); send_line(fd, "ERROR EXEC_NO_DATA"); }
                                        else {
                                            // expect: DATA <ss_id> EXEC <fname> <size>
                                            char tag[32], ssid[ID_MAX], verb2[32], fname2[FNAME_MAX]; long long size = -1;
                                            int parsed = sscanf(header, "%31s %63s %31s %255s %lld", tag, ssid, verb2, fname2, &size);
                                            if (parsed >= 4) {
                                                if (strcmp(tag,"DATA")!=0 || strcasecmp(verb2,"EXEC")!=0) {
                                                    // pipe remainder back
                                                    char buf[LINE_MAX]; ssize_t n;
                                                    while((n=recv(ssfd, buf, sizeof(buf),0))>0) send(fd, buf, n, MSG_NOSIGNAL);
                                                    close(ssfd);
                                                } else {
                                                    // read exactly 'size' bytes into temp file
                                                    char tmp_template[] = "/tmp/nm_exec_XXXXXX";
                                                    int tmpfd = mkstemp(tmp_template);
                                                    if (tmpfd<0) { close(ssfd); send_line(fd,"ERROR EXEC_TMPFAIL"); }
                                                    else {
                                                        long long toread = size;
                                                        bool size_known = (parsed == 5 && size >= 0);
                                                        bool ok = true;
                                                        char buf[4096];
                                                        while (toread != 0) {
                                                            size_t chunk = (toread < 0 || (long long)sizeof(buf) < toread) ? sizeof(buf) : (size_t)toread;
                                                            ssize_t r = recv(ssfd, buf, chunk, 0);
                                                            if (r <= 0) { ok = false; break; }
                                                            ssize_t woff = 0;
                                                            while (woff < r) {
                                                                ssize_t w = write(tmpfd, buf + woff, (size_t)(r - woff));
                                                                if (w < 0) {
                                                                    if (errno == EINTR) continue;
                                                                    ok = false;
                                                                    break;
                                                                }
                                                                woff += w;
                                                            }
                                                            if (!ok) break;
                                                            if (toread > 0) toread -= r;
                                                            if (!size_known && r < (ssize_t)sizeof(buf)) break; // heuristic break when source ends
                                                        }
                                                        fsync(tmpfd); close(tmpfd);

                                                        // consume trailing ENDDATA (skip blank lines)
                                                        char trailing[LINE_MAX];
                                                        int rl;
                                                        do {
                                                            rl = (int)read_line(ssfd, trailing, sizeof(trailing));
                                                            if (rl <= 0) break;
                                                        } while (trailing[0] == '\0');
                                                        close(ssfd);

                                                        bool got_end = (rl > 0 && strncmp(trailing, "ENDDATA", 7) == 0);
                                                        if (!got_end && size_known && toread <= 0) {
                                                            got_end = true; // treat lack of ENDDATA as success when full payload received
                                                        }
                                                        if (size_known && toread > 0) ok = false;

                                                        if (!ok || !got_end) {
                                                            send_line(fd, "ERROR EXEC_TRANSFER");
                                                            unlink(tmp_template);
                                                        } else {
                                                            // execute temp script and stream output
                                                            char cmd[PATH_MAX+32]; snprintf(cmd, sizeof(cmd), "sh %s 2>&1", tmp_template);
                                                            FILE *pf = popen(cmd, "r");
                                                            if (!pf) {
                                                                send_line(fd, "ERROR EXEC_RUN");
                                                                unlink(tmp_template);
                                                            } else {
                                                                send_line(fd, "BEGIN_EXEC_OUTPUT");
                                                                char outbuf[4096];
                                                                while (fgets(outbuf, sizeof(outbuf), pf)) {
                                                                    size_t out_len = strlen(outbuf);
                                                                    size_t sent = 0;
                                                                    while (sent < out_len) {
                                                                        ssize_t w = send(fd, outbuf + sent, out_len - sent, MSG_NOSIGNAL);
                                                                        if (w <= 0) break;
                                                                        sent += (size_t)w;
                                                                    }
                                                                }
                                                                pclose(pf);
                                                                send_line(fd, "END_EXEC_OUTPUT");
                                                                unlink(tmp_template);
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                // header not as expected: pipe everything
                                                char buf[LINE_MAX]; ssize_t n;
                                                while((n=recv(ssfd, buf, sizeof(buf),0))>0) send(fd, buf, n, MSG_NOSIGNAL);
                                                close(ssfd);
                                            }
                                        }
                                    }
                                }
                            }
                        } else send_line(fd,"ERROR BAD_EXEC");
                        close(fd); return NULL;
                    }

            if (strcasecmp(verb,"WRITE")==0) {
                char fname[FNAME_MAX]={0}; char sent[64]={0};
                if (sscanf(payload, "WRITE %255s %63s", fname, sent)==2) {
                    // handle_write drains more lines from same connection
                    handle_write(fd, client_id, fname, sent, fd);
                } else send_line(fd,"ERROR BAD_WRITE");
                close(fd); return NULL;
            }

            // Unknown verb
            send_line(fd,"ERROR unknown command");
            close(fd); return NULL;
        } else {
            send_line(fd, "ERROR malformed CLIENT");
            close(fd); return NULL;
        }
    }

    // Backward compatibility (Step-1 "REQUEST ..."): still accepted
    if (strncmp(first, "REQUEST", 7) == 0) {
        char client_id[ID_MAX]={0}; char payload[LINE_MAX]={0};
        if (sscanf(first, "REQUEST %63s %4095[^\n]", client_id, payload) >= 1) {
            // Re-inject as CLIENT line:
            char reenq[LINE_MAX]; snprintf(reenq,sizeof(reenq),"CLIENT %s %s", client_id, payload);
            // Fake a small handler by rewriting 'first' and looping once
            // Simpler: just tell user to use new client.
            send_line(fd, "ERROR Please upgrade client (use CLIENT <id> <payload>).");
        } else {
            send_line(fd, "ERROR malformed REQUEST");
        }
        close(fd); return NULL;
    }

    // Unknown
    send_line(fd, "ERROR unknown command");
    close(fd);
    return NULL;
}

// ----------------------- main -----------------------
int main(void) {
    fv_init(&ALL_FILES);
    sl_init(&USER_LIST);
    file_map_init(&FILE_MAP);
    file_cache_init(&FILE_CACHE);

    int listen_fd = create_listen_socket(NM_PORT);
    printf("Named server has started on port %d\n", NM_PORT); fflush(stdout);

    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) continue; perror("accept"); continue;
        }
        WorkerArg *wa = malloc(sizeof(*wa)); wa->fd=cfd;
        pthread_t th;
        if (pthread_create(&th, NULL, worker, wa)==0) pthread_detach(th);
        else { perror("pthread_create"); close(cfd); free(wa); }
    }

    // (not reached)
    fv_free(&ALL_FILES); sl_free(&USER_LIST);
    file_map_free(&FILE_MAP);
    return 0;
}
