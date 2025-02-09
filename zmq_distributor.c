/*************************************************************
<<<<<<< HEAD
 *  distributor_fixed.c
 *
 *  This version fixes the "Too many open files" problem by
 *  creating exactly 'n' map threads (one per worker), each of
 *  which handles multiple chunks (rather than spawning one
 *  thread/socket per chunk).
 *
 *  Comments are in Ukrainian, as requested.
=======
 *  zmq_distributor.c
 *  Логика Дистрибьютора:
 *   - чтение файла
 *   - разбиение на строки
 *   - отправка "map..." воркерам
 *   - сбор ответов, парсинг
 *   - отправка "red..." одному воркеру
 *   - финальная сортировка и вывод
 *   - "rip" всем воркерам
>>>>>>> dc6bdc5 (Test handout)
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <zmq.h>
#include <unistd.h>

<<<<<<< HEAD
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
=======
#define MSG_SIZE 1500

// -------------------------------
// Структуры
// -------------------------------
typedef struct Pair {
    char *word;
    int count;
    struct Pair *next;
} Pair;

static Pair *g_map_results = NULL;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct WorkerInfo {
    int index;
    char *endpoint;
    char *chunk;
} WorkerInfo;

void *g_zmq_context = NULL;

typedef struct FinalPair {
    char *word;
    int count;
    struct FinalPair *next;
} FinalPair;

static FinalPair *g_final_list = NULL;
static pthread_mutex_t g_final_lock = PTHREAD_MUTEX_INITIALIZER;

// -------------------------------
// Вспомогательные функции
// -------------------------------

// Добавляет (word, count) в g_map_results в конец списка
static void add_to_global_map(const char *word, int c) {
    pthread_mutex_lock(&g_lock);

    Pair *p = g_map_results;

    while (p) {
        if (strcmp(p->word, word) == 0) {
            p->count += c;
            pthread_mutex_unlock(&g_lock);
            return;
        }
        p = p->next;
    }

    Pair *newp = malloc(sizeof(*newp));
    if (!newp) {
        pthread_mutex_unlock(&g_lock);
        return;
    }

    newp->word = strdup(word);
    if (!newp->word) {
        free(newp);
        pthread_mutex_unlock(&g_lock);
        return;
    }

    newp->count = c;
    newp->next = NULL;

    if (g_map_results == NULL) {
        g_map_results = newp;
    } else {
        Pair *last = g_map_results;
        while (last->next) {
            last = last->next;
        }
        last->next = newp;
    }

    pthread_mutex_unlock(&g_lock);
}


// Парсит ответ MAP: "the11example111..."
static void parse_map_reply(const char *reply) {
    int i = 0;
    int n = (int)strlen(reply);

    while (i < n) {
        char word_buf[256];
        int wpos = 0;
        // собираем буквы
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = reply[i];
            }
            i++;
>>>>>>> dc6bdc5 (Test handout)
        }
        word_buf[wpos] = '\0';

        int count = 0;
<<<<<<< HEAD
        while (*p && *p == '1') {
            count++;
            p++;
        }

        if (wpos > 0 && count > 0) {
            pthread_mutex_lock(&global_omap_lock);
            om_update(global_omap, word_buf, count);
            pthread_mutex_unlock(&global_omap_lock);
=======
        // собираем подряд '1'
        while (i < n && reply[i] == '1') {
            count++;
            i++;
        }

        if (wpos > 0 && count > 0) {
            add_to_global_map(word_buf, count);
>>>>>>> dc6bdc5 (Test handout)
        }
    }
}

<<<<<<< HEAD
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
=======
// -------------------------------
// Поток: отправляет map<chunk>, парсит ответ
// -------------------------------
static void *map_thread(void *arg) {
    WorkerInfo *info = (WorkerInfo *)arg;

    void *req = zmq_socket(g_zmq_context, ZMQ_REQ);
    while (!req) {
        sleep(0.05);
        req = zmq_socket(g_zmq_context, ZMQ_REQ);
    }
    // if(!req){
    //     perror("zmq_socket map_thread");
    //     free(info->chunk);
    //     free(info);
    //     return NULL;
    // }
    int linger = 0;
    zmq_setsockopt(req, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(req, info->endpoint) != 0) {
        perror("zmq_connect map_thread");
        zmq_close(req);
        free(info->chunk);
        free(info);
        return NULL;
    }

    // Формируем "map" + chunk
    char msg[MSG_SIZE];
    memset(msg, 0, sizeof(msg));
    snprintf(msg, sizeof(msg), "map%s", info->chunk);
    msg[MSG_SIZE - 1]= '\0';
    // printf("Map request message len: %zu\n", strlen(msg));
    zmq_send(req, msg, strlen(msg)+1, 0);

    char reply[MSG_SIZE];
    memset(reply, 0, sizeof(reply));
    int rsize = zmq_recv(req, reply, sizeof(reply) - 1, 0);
    if (rsize > 0) {
        reply[rsize] = '\0';
        parse_map_reply(reply);
    }
    free(info->chunk);
    free(info);
>>>>>>> dc6bdc5 (Test handout)
    zmq_close(req);
    return NULL;
}

<<<<<<< HEAD
/*************************************************************
 *  build_reduce_payload: будує "red..." з global_omap
 *************************************************************/
=======
// -------------------------------
// Формируем REDUCE: "red" + (word + '1'*count)
// -------------------------------
// static void build_reduce_payload(char *out, size_t outsize) {
//     memset(out, 0, outsize);
//     strcpy(out, "red");
//     size_t pos = 3;

//     pthread_mutex_lock(&g_lock);
//     for (Pair *p = g_map_results; p != NULL; p = p->next) {
//         const char *w = p->word;
//         int wlen = (int)strlen(w);
//         if (pos + wlen < outsize - 1) {
//             memcpy(out + pos, w, wlen);
//             pos += wlen;
//         }
//         for (int i = 0; i < p->count; i++) {
//             if (pos < outsize - 1) {
//                 out[pos++] = '1';
//             } else {
//                 break;
//             }
//         }
//     }
//     pthread_mutex_unlock(&g_lock);
// }
>>>>>>> dc6bdc5 (Test handout)
static void build_reduce_payload(char *out, size_t outsize) {
    memset(out, 0, outsize);
    strcpy(out, "red");
    size_t pos = 3;
<<<<<<< HEAD

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
=======
    pthread_mutex_lock(&g_lock);
    Pair *prev = NULL; // To track the previous node for deletion if count reaches 0
    Pair *p = g_map_results;

    while (p != NULL) {
        const char *w = p->word;

        int wlen = (int)strlen(w);
        // Add the word to the payload if it fits
        if (pos + wlen < outsize - 1) {
            memcpy(out + pos, w, wlen);
            pos += wlen;
        } else {
            break; // Not enough space for this word, stop here
        }
        // Add the '1's for the current word
        while (p->count > 0 && pos < outsize - 1) {
            out[pos++] = '1';
            p->count--;
        }

        // If this word is fully processed, remove it from the list
        if (p->count == 0) {
            if (prev == NULL) {
                // Head of the list
                g_map_results = p->next;
                free(p->word);
                free(p);
                p = g_map_results;
            } else {
                // Middle or end of the list
                prev->next = p->next;
                free(p->word);
                free(p);
                p = prev->next;
            }
        } else {
            prev = p;
            p = p->next;
        }
    }
    pthread_mutex_unlock(&g_lock);
}


// Добавляет (word, c) в g_final_list
static void add_to_final_list(const char *word, int c) {
    pthread_mutex_lock(&g_final_lock);
    FinalPair *fp = g_final_list;
    while (fp) {
        if (strcmp(fp->word, word) == 0) {
            fp->count += c;
            pthread_mutex_unlock(&g_final_lock);
            return;
        }
        fp = fp->next;
    }
    FinalPair *newf = malloc(sizeof(*newf));
    newf->word = strdup(word);
    newf->count = c;
    newf->next = g_final_list;
    g_final_list = newf;
    pthread_mutex_unlock(&g_final_lock);
}

// Парсит REDUCE ответ: "the2example2..."
static void parse_reduce_reply(const char *reply) {
    int i = 0;
    int n = (int)strlen(reply);

    while (i < n) {
        char word_buf[256];
        int wpos = 0;
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = reply[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        char num_buf[64];
        int np = 0;
        while (i < n && isdigit((unsigned char)reply[i])) {
            if (np < 63) {
                num_buf[np++] = reply[i];
            }
            i++;
        }
        num_buf[np] = '\0';

        if (wpos > 0 && np > 0) {
            int c = atoi(num_buf);

            add_to_final_list(word_buf, c);
>>>>>>> dc6bdc5 (Test handout)
        }
    }
}

<<<<<<< HEAD
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
=======
// Сортируем FinalPair: убывание по частоте, потом алфавит
static int cmpfunc(const void *a, const void *b) {
    const FinalPair *fa = *(const FinalPair **)a;
    const FinalPair *fb = *(const FinalPair **)b;
    if (fa->count > fb->count) return -1;
    if (fa->count < fb->count) return +1;
    return strcmp(fa->word, fb->word);
}

// -----------------------------------------------------------------------------
// MAIN Distributor
// -----------------------------------------------------------------------------
int main(int argc, char *argv[])
{
>>>>>>> dc6bdc5 (Test handout)
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }
<<<<<<< HEAD
    int n_workers = argc - 2;

    // Формуємо endpoints
    char **endpoints = malloc(n_workers * sizeof(char*));
    for (int i = 0; i < n_workers; i++) {
=======

    int num_workers = argc - 2;
    char **endpoints = malloc(num_workers * sizeof(char*));
    for (int i = 0; i < num_workers; i++) {
>>>>>>> dc6bdc5 (Test handout)
        char buf[64];
        snprintf(buf, sizeof(buf), "tcp://localhost:%s", argv[i+2]);
        endpoints[i] = strdup(buf);
    }

<<<<<<< HEAD
    // Створюємо ZeroMQ контекст
=======
>>>>>>> dc6bdc5 (Test handout)
    g_zmq_context = zmq_ctx_new();
    if (!g_zmq_context) {
        fprintf(stderr, "zmq_ctx_new error\n");
        return 1;
    }

<<<<<<< HEAD
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
=======
    const char *filename = argv[1];
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *filecontent = malloc(fsize + 1);
    if (!filecontent) {
        fprintf(stderr, "Not enough memory\n");
        fclose(fp);
        return 1;
    }
    fread(filecontent, 1, fsize, fp);
    filecontent[fsize] = '\0';
    fclose(fp);

    // Стартуем потоки map (по строкам)
    pthread_t threads[1024];
    int thread_count = 0;

    char *file_ptr = filecontent; // Pointer to track the current position in filecontent
    size_t chunk_size = 1496;    // Maximum chunk size
    while (*file_ptr) {
        WorkerInfo *wi = malloc(sizeof(*wi));

        // Choose a worker in round-robin fashion
        static int worker_index = 0;
        int widx = worker_index % num_workers;
        worker_index++;

        wi->index = widx;
        wi->endpoint = endpoints[widx];

        // Extract up to 1497 characters
        size_t len = strlen(file_ptr);
        size_t actual_chunk_size = (len > chunk_size) ? chunk_size : len;

        // Ensure the chunk ends at a space if it's not the last part of the file
        if (actual_chunk_size < len && file_ptr[actual_chunk_size] != ' ') {
            while (actual_chunk_size > 0 && file_ptr[actual_chunk_size - 1] != ' ') {
                actual_chunk_size--;
            }
        }

        // Create a new chunk with the extracted part
        wi->chunk = strndup(file_ptr, actual_chunk_size);

        // Update the file pointer to the next part of the file
        file_ptr += actual_chunk_size;

        // Skip leading spaces to start at the next word
        while (*file_ptr == ' ') {
            file_ptr++;
        }

        // Create the thread to process this chunk
        pthread_create(&threads[thread_count], NULL, map_thread, wi);
        thread_count++;
    }


    // Ждём завершения map-потоков
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }


    // Выполняем reduce, используем воркер[0]
    // void *reduce_socket = zmq_socket(g_zmq_context, ZMQ_REQ);
    // if (!reduce_socket) {
    //     perror("zmq_socket reduce");
    //     return 1;
    // }
    // int linger = 0;
    // zmq_setsockopt(reduce_socket, ZMQ_LINGER, &linger, sizeof(linger));

    // if (zmq_connect(reduce_socket, endpoints[0]) != 0) {
    //     perror("zmq_connect reduce");
    //     return 1;
    // }

    // char reduce_msg[MSG_SIZE];

    // build_reduce_payload(reduce_msg, MSG_SIZE);
    // zmq_send(reduce_socket, reduce_msg, strlen(reduce_msg), 0);

    // char reduce_reply[MSG_SIZE];
    // memset(reduce_reply, 0, sizeof(reduce_reply));
    // int r = zmq_recv(reduce_socket, reduce_reply, MSG_SIZE - 1, 0);
    // if (r > 0) {
    //     reduce_reply[r] = '\0';
    //     parse_reduce_reply(reduce_reply);
    // }

    // zmq_close(reduce_socket);
    void *reduce_socket = zmq_socket(g_zmq_context, ZMQ_REQ);
    if (!reduce_socket) {
        perror("zmq_socket reduce");
        return 1;
    }

    int linger = 0;
    zmq_setsockopt(reduce_socket, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(reduce_socket, endpoints[0]) != 0) {
>>>>>>> dc6bdc5 (Test handout)
        perror("zmq_connect reduce");
        return 1;
    }

<<<<<<< HEAD
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
=======
    char reduce_msg[MSG_SIZE];

    while (1) {
        pthread_mutex_lock(&g_lock);
        if (g_map_results == NULL) {
            pthread_mutex_unlock(&g_lock);
            break; // Exit the loop when g_map_results is empty
        }
        pthread_mutex_unlock(&g_lock);

        // Build the reduce payload
        memset(reduce_msg, 0, sizeof(reduce_msg));
        build_reduce_payload(reduce_msg, MSG_SIZE);
        reduce_msg[MSG_SIZE - 1] = '\0';
        // printf("Reduce message len: %zu\n", strlen(reduce_msg));
        // Send the reduce message to the worker
        if (zmq_send(reduce_socket, reduce_msg, strlen(reduce_msg)+1, 0) == -1) {
            perror("zmq_send reduce");
            break;
        }

        // Receive the reduce reply from the worker
        char reduce_reply[MSG_SIZE];
        memset(reduce_reply, 0, sizeof(reduce_reply));
        int r = zmq_recv(reduce_socket, reduce_reply, MSG_SIZE - 1, 0);
        if (r > 0) {
            reduce_reply[r] = '\0';
            parse_reduce_reply(reduce_reply);
        } else if (r == -1) {
            perror("zmq_recv reduce");
            break;
        }
    }


    zmq_close(reduce_socket);

    // KILL THEM ALLL
    for (int i = 0; i < num_workers; i++) {
        fprintf(stderr, "Sending rip");
        void *s = zmq_socket(g_zmq_context, ZMQ_REQ);
        if (!s) {
            perror("zmq_socket rip");
            zmq_close(s);
            zmq_ctx_destroy(g_zmq_context);
            return 1;
        }
        zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
        if (zmq_connect(s, endpoints[i]) != 0) {
            perror("zmq_connect rip");
            zmq_close(s);
            zmq_ctx_destroy(g_zmq_context);
            return 1;
        }

        zmq_send(s, "rip\0", 4, 0);

        char rbuf[MSG_SIZE];
        int rr2 = zmq_recv(s, rbuf, MSG_SIZE - 1, 0);
        if (rr2 > 0) {
            rbuf[rr2] = '\0';
            // ожидаем "rip"
        }

        zmq_close(s);

    }

    zmq_ctx_destroy(g_zmq_context);

    // Собираем и сортируем результаты
    int count_final = 0;
    pthread_mutex_lock(&g_final_lock);
    {
        FinalPair *fp2 = g_final_list;
        while (fp2) {
            count_final++;
            fp2 = fp2->next;
        }
    }
    pthread_mutex_unlock(&g_final_lock);

    FinalPair **arr = malloc(count_final * sizeof(FinalPair*));
    pthread_mutex_lock(&g_final_lock);
    {
        FinalPair *fp2 = g_final_list;
        int idx = 0;
        while (fp2) {
            arr[idx++] = fp2;
            fp2 = fp2->next;
        }
    }
    pthread_mutex_unlock(&g_final_lock);

    qsort(arr, count_final, sizeof(FinalPair*), cmpfunc);
    fprintf(stderr, "Output of %d words", count_final);

    // Печатаем CSV
    printf("word,frequency\n");
    for (int i = 0; i < count_final; i++) {
        printf("%s,%d\n", arr[i]->word, arr[i]->count);

    }
    fprintf(stderr, "Printed csv");
    free(arr);


    // for (int i = 0; i < num_workers; i++) {
    //     void *s = zmq_socket(g_zmq_context, ZMQ_REQ);
    //     if (!s) {
    //         perror("zmq_socket rip");
    //         return 1;
    //     }
    //     if (s) {

    //         zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
    //         if (zmq_connect(s, endpoints[i]) == 0) {

    //             zmq_send(s, "rip\0", 4, 0);

    //             char rbuf[MSG_SIZE];
    //             int rr2 = zmq_recv(s, rbuf, MSG_SIZE - 1, 0);
    //             if (rr2 > 0) {
    //                 rbuf[rr2] = '\0';
    //                 // ожидаем "rip"
    //             }
    //         }
    //         zmq_close(s);
    //     }
    // }



    // Очистка памяти
    for (int i = 0; i < num_workers; i++) {
        free(endpoints[i]);
    }
    free(endpoints);
    free(filecontent);
>>>>>>> dc6bdc5 (Test handout)

    return 0;
}
