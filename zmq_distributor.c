/*************************************************************
 *  distributor_fixed.c
 *
 *  This version fixes the "Too many open files" problem by
 *  creating exactly 'n' map threads (one per worker), each of
 *  which handles multiple chunks (rather than spawning one
 *  thread/socket per chunk).
 *
 *  Comments are in Ukrainian, as requested.
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <zmq.h>
#include <unistd.h>

#define MAX_MSG_SIZE 1500
#define HASH_SIZE 1024

/*************************************************************
 *  СТРУКТУРИ ТА ФУНКЦІЇ ДЛЯ OrderedMap (проміжна мапа)
 *************************************************************/
typedef struct OMNode {
    char *word;
    int count;
    struct OMNode *bucket_next;
    struct OMNode *order_next;
} OMNode;

typedef struct OrderedMap {
    OMNode **buckets;
    OMNode *order_head;
    OMNode *order_tail;
} OrderedMap;

// djb2 hash
static unsigned int om_hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c;
    return (unsigned int)(hash % HASH_SIZE);
}

static OrderedMap *om_create(void) {
    OrderedMap *om = malloc(sizeof(OrderedMap));
    if (!om) return NULL;
    om->buckets = calloc(HASH_SIZE, sizeof(OMNode *));
    om->order_head = NULL;
    om->order_tail = NULL;
    return om;
}

static void om_update(OrderedMap *om, const char *word, int count) {
    unsigned int idx = om_hash(word);
    OMNode *node = om->buckets[idx];
    while (node) {
        if (strcmp(node->word, word) == 0) {
            node->count += count;
            return;
        }
        node = node->bucket_next;
    }
    OMNode *new_node = malloc(sizeof(OMNode));
    new_node->word = strdup(word);
    new_node->count = count;
    new_node->bucket_next = om->buckets[idx];
    om->buckets[idx] = new_node;
    new_node->order_next = NULL;
    if (om->order_tail) {
        om->order_tail->order_next = new_node;
        om->order_tail = new_node;
    } else {
        om->order_head = new_node;
        om->order_tail = new_node;
    }
}

static void om_remove(OrderedMap *om, const char *word) {
    unsigned int idx = om_hash(word);
    OMNode **pp = &om->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->word, word) == 0) {
            OMNode *to_delete = *pp;
            *pp = to_delete->bucket_next;
            // Видаляємо з ланцюжка order
            if (om->order_head == to_delete) {
                om->order_head = to_delete->order_next;
                if (om->order_tail == to_delete)
                    om->order_tail = NULL;
            } else {
                OMNode *prev = om->order_head;
                while (prev && prev->order_next != to_delete)
                    prev = prev->order_next;
                if (prev) {
                    prev->order_next = to_delete->order_next;
                    if (om->order_tail == to_delete)
                        om->order_tail = prev;
                }
            }
            free(to_delete->word);
            free(to_delete);
            return;
        }
        pp = &((*pp)->bucket_next);
    }
}

static void om_free(OrderedMap *om) {
    if (!om) return;
    OMNode *node = om->order_head;
    while (node) {
        OMNode *tmp = node;
        node = node->order_next;
        free(tmp->word);
        free(tmp);
    }
    free(om->buckets);
    free(om);
}

/*************************************************************
 *  СТРУКТУРИ ТА ФУНКЦІЇ ДЛЯ фінального HashMap
 *************************************************************/
typedef struct FPNode {
    char *word;
    int count;
    struct FPNode *next;
} FPNode;

typedef struct HashMap {
    FPNode **buckets;
} HashMap;

static HashMap *hm_create(void) {
    HashMap *map = malloc(sizeof(HashMap));
    if (!map) return NULL;
    map->buckets = calloc(HASH_SIZE, sizeof(FPNode *));
    return map;
}

static unsigned int final_hash(const char *word) {
    unsigned long hash = 5381;
    const unsigned char *ptr = (const unsigned char *)word;
    while (*ptr)
        hash = ((hash << 5) + hash) + *ptr++;
    return (unsigned int)(hash % HASH_SIZE);
}

static void hm_update(HashMap *map, const char *word, int count) {
    unsigned int idx = final_hash(word);
    FPNode *node = map->buckets[idx];
    while (node) {
        if (strcmp(node->word, word) == 0) {
            node->count += count;
            return;
        }
        node = node->next;
    }
    FPNode *newnode = malloc(sizeof(FPNode));
    newnode->word = strdup(word);
    newnode->count = count;
    newnode->next = map->buckets[idx];
    map->buckets[idx] = newnode;
}

static void hm_free(HashMap *map) {
    if (!map) return;
    for (int i = 0; i < HASH_SIZE; i++) {
        FPNode *node = map->buckets[i];
        while (node) {
            FPNode *tmp = node;
            node = node->next;
            free(tmp->word);
            free(tmp);
        }
    }
    free(map->buckets);
    free(map);
}

/*************************************************************
 *  Глобальні змінні та мʼютекси
 *************************************************************/
static OrderedMap *global_omap = NULL;
static pthread_mutex_t global_omap_lock = PTHREAD_MUTEX_INITIALIZER;

static HashMap *global_hash_map = NULL;
static pthread_mutex_t global_hash_lock = PTHREAD_MUTEX_INITIALIZER;

static void *g_zmq_context = NULL;

/*************************************************************
 *  Структура для даних потоку (один потік на кожного воркера)
 *************************************************************/
typedef struct WorkerThreadData {
    int worker_index;           // Індекс воркера (0..n-1)
    char *endpoint;             // "tcp://localhost:XXXX"
    // Нижче — інформація про всі chunks:
    char **chunk_array;         // Усі згенеровані частини
    int total_chunks;           // Загальна кількість частин
    int n_workers;              // Кількість воркерів
} WorkerThreadData;

/*************************************************************
 *  aggregate_map_reply: розбирає "word111word111..." та
 *  оновлює global_omap під мʼютексом
 *************************************************************/
static void aggregate_map_reply(const char *reply) {
    const char *p = reply;
    while (*p != '\0') {
        char word_buf[256];
        int wpos = 0;
        while (*p && isalpha((unsigned char)*p)) {
            if (wpos < 255) {
                word_buf[wpos++] = *p;
            }
            p++;
        }
        word_buf[wpos] = '\0';

        int count = 0;
        while (*p && *p == '1') {
            count++;
            p++;
        }

        if (wpos > 0 && count > 0) {
            pthread_mutex_lock(&global_omap_lock);
            om_update(global_omap, word_buf, count);
            pthread_mutex_unlock(&global_omap_lock);
        }
    }
}

/*************************************************************
 *  Потік для map-фази: Один потік на одного воркера.
 *  Цей потік проходить по масиву частин, відбираючи "свої"
 *  за індексом (round-robin або i + k*n), надсилає їх на worker,
 *  і обробляє відповіді.
 *************************************************************/
static void *map_thread_func(void *arg) {
    WorkerThreadData *td = (WorkerThreadData *)arg;

    // Створюємо один ZMQ_REQ сокет для цього воркера
    void *req = zmq_socket(g_zmq_context, ZMQ_REQ);
    if (!req) {
        perror("zmq_socket map_thread");
        return NULL;
    }
    int linger = 0;
    zmq_setsockopt(req, ZMQ_LINGER, &linger, sizeof(linger));
    if (zmq_connect(req, td->endpoint) != 0) {
        perror("zmq_connect map_thread");
        zmq_close(req);
        return NULL;
    }

    // Проходимо усі chunks, але опрацьовуємо лише ті, які належать
    // цьому worker_index (наприклад, chunk #0 -> worker0, #1->worker1, ...)
    for (int i = td->worker_index; i < td->total_chunks; i += td->n_workers) {
        // Будуємо повідомлення "map + chunk"
        char *chunk = td->chunk_array[i];
        if (!chunk) continue; // safety check

        char msg[MAX_MSG_SIZE];
        snprintf(msg, sizeof(msg), "map%s", chunk);

        // Надсилаємо
        zmq_send(req, msg, strlen(msg) + 1, 0);

        // Чекаємо відповіді
        char reply[MAX_MSG_SIZE];
        memset(reply, 0, sizeof(reply));
        int rsize = zmq_recv(req, reply, sizeof(reply) - 1, 0);
        if (rsize > 0) {
            reply[rsize] = '\0';
            // Парсимо та агрегуємо
            aggregate_map_reply(reply);
        }
    }

    // Закриваємо цей сокет
    zmq_close(req);
    return NULL;
}

/*************************************************************
 *  build_reduce_payload: будує "red..." з global_omap
 *************************************************************/
static void build_reduce_payload(char *out, size_t outsize) {
    memset(out, 0, outsize);
    strcpy(out, "red");
    size_t pos = 3;

    pthread_mutex_lock(&global_omap_lock);
    OMNode *curr = global_omap->order_head;
    OMNode *prev = NULL;
    while (curr && pos < outsize - 1) {
        int wlen = (int)strlen(curr->word);
        if (pos + wlen >= outsize - 1)
            break;
        memcpy(out + pos, curr->word, wlen);
        pos += wlen;

        while (curr->count > 0 && pos < outsize - 1) {
            out[pos++] = '1';
            curr->count--;
        }

        if (curr->count == 0) {
            char *temp_key = strdup(curr->word);
            if (!prev) {
                global_omap->order_head = curr->order_next;
                curr = global_omap->order_head;
            } else {
                prev->order_next = curr->order_next;
                curr = prev->order_next;
            }
            om_remove(global_omap, temp_key);
            free(temp_key);
        } else {
            prev = curr;
            curr = curr->order_next;
        }
    }
    pthread_mutex_unlock(&global_omap_lock);
    out[outsize - 1] = '\0';
}

/*************************************************************
 *  parse_reduce_reply: розбирає "word<number>" і оновлює
 *  глобальну фінальну HashMap
 *************************************************************/
static void parse_reduce_reply(const char *reply) {
    int i = 0;
    int n = (int)strlen(reply);
    while (i < n) {
        char wbuf[256];
        int wpos = 0;
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255)
                wbuf[wpos++] = reply[i];
            i++;
        }
        wbuf[wpos] = '\0';

        char nbuf[64];
        int np = 0;
        while (i < n && isdigit((unsigned char)reply[i])) {
            if (np < 63)
                nbuf[np++] = reply[i];
            i++;
        }
        nbuf[np] = '\0';

        if (wpos > 0 && np > 0) {
            int c = atoi(nbuf);
            pthread_mutex_lock(&global_hash_lock);
            hm_update(global_hash_map, wbuf, c);
            pthread_mutex_unlock(&global_hash_lock);
        }
    }
}

/*************************************************************
 *  Компаратор для фінального сортування
 *************************************************************/
static int cmp_final(const void *a, const void *b) {
    const FPNode *fa = *(const FPNode **)a;
    const FPNode *fb = *(const FPNode **)b;
    if (fa->count > fb->count) return -1;
    if (fa->count < fb->count) return 1;
    return strcmp(fa->word, fb->word);
}

/*************************************************************
 *  MAIN
 *************************************************************/
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }
    int n_workers = argc - 2;

    // Формуємо endpoints
    char **endpoints = malloc(n_workers * sizeof(char*));
    for (int i = 0; i < n_workers; i++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "tcp://localhost:%s", argv[i+2]);
        endpoints[i] = strdup(buf);
    }

    // Створюємо ZeroMQ контекст
    g_zmq_context = zmq_ctx_new();
    if (!g_zmq_context) {
        fprintf(stderr, "zmq_ctx_new error\n");
        return 1;
    }

    // Читаємо файл
    const char *filename = argv[1];
    FILE *f = fopen(filename, "r");
    if (!f) {
        perror("fopen");
        return 1;
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *file_content = malloc(fsize + 1);
    if (!file_content) {
        fclose(f);
        fprintf(stderr, "Not enough memory\n");
        return 1;
    }
    fread(file_content, 1, fsize, f);
    file_content[fsize] = '\0';
    fclose(f);

    // Створюємо проміжну карту та фінальну
    global_omap = om_create();
    global_hash_map = hm_create();

    // Спочатку розіб'ємо весь текст на chunks (не розриваючи слова)
    // Але тепер не запускаємо потік на кожну частину; просто зберігаємо їх
    size_t chunk_size = 1496;  // "map" + payload fits in 1500
    char *ptr = file_content;

    // Зберігатимемо всі знайдені частини у динамічному масиві
    // Припустимо максимум ~65536 частин
    char **chunk_array = malloc(sizeof(char*) * 65536);
    int total_chunks = 0;

    while (*ptr) {
        size_t len = strlen(ptr);
        if (len == 0) break;
        size_t actual = (len > chunk_size) ? chunk_size : len;
        // Не розбивати слова
        if (actual < len && ptr[actual] != ' ') {
            while (actual > 0 && ptr[actual - 1] != ' ')
                actual--;
        }
        chunk_array[total_chunks] = strndup(ptr, actual);
        total_chunks++;
        ptr += actual;
        while (*ptr == ' ') ptr++; // skip spaces
    }

    // Тепер запускаємо n потоків, по одному на кожен worker
    pthread_t *threads = malloc(n_workers * sizeof(pthread_t));
    WorkerThreadData *td_list = malloc(n_workers * sizeof(WorkerThreadData));

    for (int i = 0; i < n_workers; i++) {
        td_list[i].worker_index = i;
        td_list[i].endpoint = endpoints[i];
        td_list[i].chunk_array = chunk_array;
        td_list[i].total_chunks = total_chunks;
        td_list[i].n_workers = n_workers;
        pthread_create(&threads[i], NULL, map_thread_func, &td_list[i]);
    }

    // Чекаємо завершення map-потоків
    for (int i = 0; i < n_workers; i++) {
        pthread_join(threads[i], NULL);
    }

    // Після map-фази виконуємо reduce: підключаємося до ПЕРШОГО воркера
    void *reduce_sock = zmq_socket(g_zmq_context, ZMQ_REQ);
    if (!reduce_sock) {
        perror("zmq_socket reduce");
        return 1;
    }
    int linger = 0;
    zmq_setsockopt(reduce_sock, ZMQ_LINGER, &linger, sizeof(linger));
    if (zmq_connect(reduce_sock, endpoints[0]) != 0) {
        perror("zmq_connect reduce");
        return 1;
    }

    char reduce_msg[MAX_MSG_SIZE];
    while (1) {
        pthread_mutex_lock(&global_omap_lock);
        int empty = (global_omap->order_head == NULL);
        pthread_mutex_unlock(&global_omap_lock);
        if (empty) break;

        build_reduce_payload(reduce_msg, MAX_MSG_SIZE);
        if (zmq_send(reduce_sock, reduce_msg, strlen(reduce_msg) + 1, 0) == -1) {
            perror("zmq_send reduce");
            break;
        }
        char reduce_reply[MAX_MSG_SIZE];
        memset(reduce_reply, 0, sizeof(reduce_reply));
        int r = zmq_recv(reduce_sock, reduce_reply, MAX_MSG_SIZE - 1, 0);
        if (r > 0) {
            reduce_reply[r] = '\0';
            parse_reduce_reply(reduce_reply);
        }
    }
    zmq_close(reduce_sock);

    // Надсилаємо "rip" усім воркерам
    for (int i = 0; i < n_workers; i++) {
        void *s = zmq_socket(g_zmq_context, ZMQ_REQ);
        if (!s) {
            perror("rip zmq_socket");
            continue;
        }
        zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
        if (zmq_connect(s, endpoints[i]) == 0) {
            zmq_send(s, "rip", 4, 0);
            char rbuf[MAX_MSG_SIZE];
            zmq_recv(s, rbuf, MAX_MSG_SIZE - 1, 0); // just ignore content
        }
        zmq_close(s);
    }

    // Звільняємо контекст
    zmq_ctx_destroy(g_zmq_context);

    // Формуємо фінальний список для сортування
    int total_words = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        FPNode *node = global_hash_map->buckets[i];
        while (node) {
            total_words++;
            node = node->next;
        }
    }
    FPNode **arr = malloc(total_words * sizeof(FPNode*));
    int idx = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        FPNode *node = global_hash_map->buckets[i];
        while (node) {
            arr[idx++] = node;
            node = node->next;
        }
    }
    qsort(arr, total_words, sizeof(FPNode*), cmp_final);

    printf("word,frequency\n");
    for (int i = 0; i < total_words; i++) {
        printf("%s,%d\n", arr[i]->word, arr[i]->count);
    }

    // Прибирання
    free(arr);
    for (int i = 0; i < n_workers; i++) {
        free(endpoints[i]);
    }
    free(endpoints);

    for (int i = 0; i < total_chunks; i++) {
        free(chunk_array[i]);
    }
    free(chunk_array);
    free(file_content);
    free(threads);
    free(td_list);

    om_free(global_omap);
    hm_free(global_hash_map);

    return 0;
}
