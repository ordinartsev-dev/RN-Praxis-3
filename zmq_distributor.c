/*************************************************************
 *  zmq_distributor.c
 *  Логика Дистрибьютора:
 *   - чтение файла
 *   - разбиение на куски (chunk_size ~ 1496, чтобы влезало "map")
 *   - запуск num_workers потоков (по одному на каждого воркера)
 *   - каждый поток последовательно отправляет "map<...>" своему воркеру
 *   - сбор ответов, парсинг
 *   - reduce (пока есть данные) "red..." к одному воркеру
 *   - финальная сортировка и вывод
 *   - "rip" всем воркерам
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <stdarg.h>
#include <zmq.h>
#include <unistd.h>

#define MSG_SIZE 1500

// -------------------------------
// Определим путь к файлу отладки
// -------------------------------
#define DEBUG_LOGFILE "debug_output.txt"

// Утилита для записи отладочной информации в файл
static void debug_log(const char *fmt, ...)
{
    FILE *f = fopen(DEBUG_LOGFILE, "a");  // открываем на добавление
    if (!f) return;  // если не удалось открыть, просто пропускаем

    va_list args;
    va_start(args, fmt);
    vfprintf(f, fmt, args);
    va_end(args);

    fclose(f);
}

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

// Список из чанков
static char **g_chunks = NULL;
static size_t g_num_chunks = 0;
static size_t g_chunks_capacity = 0;

typedef struct WorkerTask {
    int worker_index;     // к какому воркеру/endpoint подключаться
    char *endpoint;       // tcp://localhost:port
    int thread_index;     // индекс самого потока (0..num_workers-1)
    int total_workers;    // всего воркеров = num_workers
} WorkerTask;

void *g_zmq_context = NULL;

typedef struct FinalPair {
    char *word;
    int count;
    struct FinalPair *next;
} FinalPair;

static FinalPair *g_final_list = NULL;
static pthread_mutex_t g_final_lock = PTHREAD_MUTEX_INITIALIZER;

// -------------------------------
// Добавляем (word, count) в g_map_results
// -------------------------------
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
    newp->word = strdup(word);
    newp->count = c;
    newp->next = g_map_results;
    g_map_results = newp;
    pthread_mutex_unlock(&g_lock);
}

// Парсит ответ MAP: "the11example111..."
static void parse_map_reply(const char *reply) {
    debug_log("[parse_map_reply] worker reply='%s'\n", reply);

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

        int count = 0;
        while (i < n && reply[i] == '1') {
            count++;
            i++;
        }

        if (wpos > 0 && count > 0) {
            add_to_global_map(word_buf, count);
        }
    }
}

// -------------------------------
// Поток: отправляет map<chunk>, парсит ответ
// -------------------------------
static void *map_worker_thread(void *arg) {
    WorkerTask *task = (WorkerTask *)arg;
    int my_index = task->thread_index;
    int step = task->total_workers;

    void *req = zmq_socket(g_zmq_context, ZMQ_REQ);
    if(!req){
        perror("zmq_socket map_thread");
        return NULL;
    }
    int linger = 0;
    zmq_setsockopt(req, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(req, task->endpoint) != 0) {
        perror("zmq_connect map_thread");
        zmq_close(req);
        return NULL;
    }

    // Перебираем все чанки, которые должен обработать этот поток
    for (size_t i = my_index; i < g_num_chunks; i += step) {
        char msg[MSG_SIZE];
        memset(msg, 0, sizeof(msg));
        snprintf(msg, sizeof(msg), "map%s", g_chunks[i]);

        debug_log("[map_worker_thread] thread=%d worker=%d sending='%s'\n",
                  my_index, task->worker_index, msg);

        // Отправляем
        zmq_send(req, msg, strlen(msg), 0);

        // Получаем ответ
        char reply[MSG_SIZE];
        memset(reply, 0, sizeof(reply));
        int rsize = zmq_recv(req, reply, sizeof(reply) - 1, 0);
        if (rsize > 0) {
            reply[rsize] = '\0';
            parse_map_reply(reply);
        }
    }

    zmq_close(req);
    return NULL;
}

// -------------------------------
// Формируем REDUCE: "red" + (word + '1'*count)
// -------------------------------
static void build_reduce_payload(char *out, size_t outsize) {
    memset(out, 0, outsize);
    strcpy(out, "red");
    size_t pos = 3;

    pthread_mutex_lock(&g_lock);
    Pair *prev = NULL;
    Pair *p = g_map_results;

    // Допустим, слова, которые сами по себе длиннее, чем outsize - 4, выкидываем
    size_t max_word_len = outsize - 4; // "red"+'\0' → ~4 байта

    while (p != NULL) {
        const char *w = p->word;
        int wlen = (int)strlen(w);

        // Если слово само по себе слишком велико -> убираем
        if (wlen > (int)max_word_len) {
            debug_log("[build_reduce_payload] removing oversize word='%s'\n", w);
            if (prev == NULL) {
                g_map_results = p->next;
            } else {
                prev->next = p->next;
            }
            free(p->word);
            Pair *tmp = p;
            p = p->next;
            free(tmp);
            continue;
        }

        // Если сейчас слово не влезает в остаток => break, НЕ удаляем
        if (pos + wlen >= outsize - 1) {
            debug_log("[build_reduce_payload] word='%s' doesn't fit => break\n", w);
            break;
        }

        // Копируем слово
        memcpy(out + pos, w, wlen);
        pos += wlen;

        // Пытаемся дописать '1' p->count раз
        while (p->count > 0 && pos < outsize - 1) {
            out[pos++] = '1';
            p->count--;
        }

        // Если все '1' дописали -> удаляем это слово
        if (p->count == 0) {
            if (prev == NULL) {
                g_map_results = p->next;
                free(p->word);
                free(p);
                p = g_map_results;
            } else {
                prev->next = p->next;
                free(p->word);
                free(p);
                p = prev->next;
            }
        } else {
            // Выходим, чтобы в следующем проходе дописать
            debug_log("[build_reduce_payload] partial leftover on word='%s'\n", w);
            prev = p;
            p = p->next;
            break;
        }

        if (pos >= outsize - 1) {
            break;
        }
    }

    pthread_mutex_unlock(&g_lock);

    debug_log("[build_reduce_payload] result='%s'\n", out);
}

// -------------------------------
// Добавляем (word, c) в g_final_list
// -------------------------------
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
    debug_log("[parse_reduce_reply] worker reply='%s'\n", reply);

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
        }
    }
}

// Сортируем FinalPair: убывание по count, при равенстве — алфавит
static int cmpfunc(const void *a, const void *b) {
    const FinalPair *fa = *(const FinalPair **)a;
    const FinalPair *fb = *(const FinalPair **)b;
    if (fa->count > fb->count) return -1;
    if (fa->count < fb->count) return +1;
    return strcmp(fa->word, fb->word);
}

// -------------------------------
// Функция разбивки filecontent на чанки
// -------------------------------
static void add_chunk(char *start, size_t len) {
    if (g_num_chunks == g_chunks_capacity) {
        size_t newcap = (g_chunks_capacity == 0) ? 256 : g_chunks_capacity * 2;
        g_chunks = realloc(g_chunks, newcap * sizeof(*g_chunks));
        g_chunks_capacity = newcap;
    }
    g_chunks[g_num_chunks] = strndup(start, len);

    debug_log("[split_into_chunks] chunk#%zu='%s'\n", g_num_chunks, g_chunks[g_num_chunks]);

    g_num_chunks++;
}

static void split_into_chunks(char *filecontent, size_t chunk_size) {
    debug_log("\n\n=============================\nNEW RUN of Distributor\n=============================\n");
    debug_log("=== Starting to split the file into chunks (chunk_size=%zu) ===\n", chunk_size);

    char *ptr = filecontent;
    while (*ptr) {
        size_t len = strlen(ptr);
        if (len == 0) {
            break;
        }
        size_t actual_chunk_size = (len > chunk_size) ? chunk_size : len;

        if (actual_chunk_size < len && ptr[actual_chunk_size] != ' ') {
            size_t tmp = actual_chunk_size;
            while (tmp > 0 && ptr[tmp - 1] != ' ') {
                tmp--;
            }
            if (tmp > 0) {
                actual_chunk_size = tmp;
            }
        }

        add_chunk(ptr, actual_chunk_size);

        ptr += actual_chunk_size;
        while (*ptr == ' ') {
            ptr++;
        }
    }
    debug_log("=== Done splitting into %zu chunks ===\n", g_num_chunks);
}

// -------------------------------
// MAIN Distributor
// -------------------------------
int main(int argc, char *argv[])
{
    // Очистим debug-файл в начале (чтобы каждый запуск показывалось с чистого листа).
    // Можно убрать если хотите накапливать логи
    FILE *df = fopen(DEBUG_LOGFILE, "w");
    if (df) fclose(df);

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }

    int num_workers = argc - 2;
    char **endpoints = malloc(num_workers * sizeof(char*));
    for (int i = 0; i < num_workers; i++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "tcp://localhost:%s", argv[i+2]);
        endpoints[i] = strdup(buf);
    }

    g_zmq_context = zmq_ctx_new();
    if (!g_zmq_context) {
        fprintf(stderr, "zmq_ctx_new error\n");
        return 1;
    }

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

    debug_log("=== Original file content (size=%ld) ===\n%s\n=== End of file content ===\n",
              fsize, filecontent);

    // Разбиваем на чанки
    split_into_chunks(filecontent, 1496);

    // Запускаем map-потоки
    pthread_t *threads = malloc(num_workers * sizeof(pthread_t));
    WorkerTask *tasks = malloc(num_workers * sizeof(WorkerTask));
    for (int i = 0; i < num_workers; i++) {
        tasks[i].worker_index = i;
        tasks[i].endpoint = endpoints[i];
        tasks[i].thread_index = i;
        tasks[i].total_workers = num_workers;
        pthread_create(&threads[i], NULL, map_worker_thread, &tasks[i]);
    }

    // Ждём map-потоки
    for (int i = 0; i < num_workers; i++) {
        pthread_join(threads[i], NULL);
    }

    // reduce: используем endpoints[0]
    void *reduce_socket = zmq_socket(g_zmq_context, ZMQ_REQ);
    if (!reduce_socket) {
        perror("zmq_socket reduce");
        return 1;
    }
    int linger = 0;
    zmq_setsockopt(reduce_socket, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(reduce_socket, endpoints[0]) != 0) {
        perror("zmq_connect reduce");
        return 1;
    }

    while (1) {
        pthread_mutex_lock(&g_lock);
        int empty = (g_map_results == NULL);
        pthread_mutex_unlock(&g_lock);

        if (empty) {
            break;
        }

        int rcvtimeo = 2000;
        zmq_setsockopt(reduce_socket, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(rcvtimeo));

        char reduce_msg[MSG_SIZE];
        build_reduce_payload(reduce_msg, MSG_SIZE);

        debug_log("[REDUCE send] '%s'\n", reduce_msg);

        if (zmq_send(reduce_socket, reduce_msg, strlen(reduce_msg), 0) == -1) {
            perror("zmq_send reduce");
            break;
        }

        char reduce_reply[MSG_SIZE];
        memset(reduce_reply, 0, sizeof(reduce_reply));
        int r = zmq_recv(reduce_socket, reduce_reply, MSG_SIZE - 1, 0);
        if (r > 0) {
            reduce_reply[r] = '\0';
            debug_log("[REDUCE recv] '%s'\n", reduce_reply);
            parse_reduce_reply(reduce_reply);
        } else if (r == -1) {
            perror("zmq_recv reduce");
            break;
        }
    }

    zmq_close(reduce_socket);

    // Сортируем финальный результат
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

    debug_log("=== Final reduce results ===\n");
    for (int i = 0; i < count_final; i++) {
        debug_log("%s,%d\n", arr[i]->word, arr[i]->count);
    }

    printf("word,frequency\n");
    for (int i = 0; i < count_final; i++) {
        printf("%s,%d\n", arr[i]->word, arr[i]->count);
    }
    free(arr);

    // rip всем воркерам
    for (int i = 0; i < num_workers; i++) {
        void *s = zmq_socket(g_zmq_context, ZMQ_REQ);
        if (s) {
            zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
            if (zmq_connect(s, endpoints[i]) == 0) {
                zmq_send(s, "rip", 3, 0);

                char rbuf[MSG_SIZE];
                int rr2 = zmq_recv(s, rbuf, MSG_SIZE - 1, 0);
                if (rr2 > 0) {
                    rbuf[rr2] = '\0';
                }
            }
            zmq_close(s);
        }
    }

    zmq_ctx_destroy(g_zmq_context);

    for (int i = 0; i < num_workers; i++) {
        free(endpoints[i]);
    }
    free(endpoints);
    free(tasks);
    free(threads);

    for (size_t i = 0; i < g_num_chunks; i++) {
        free(g_chunks[i]);
    }
    free(g_chunks);
    free(filecontent);

    return 0;
}
