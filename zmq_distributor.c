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
#include <zmq.h>
#include <unistd.h>

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
// Вспомогательные функции
// -------------------------------

// Добавляет (word, count) в g_map_results
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
    int i = 0;
    int n = (int)strlen(reply);

    while (i < n) {
        // Собираем слово
        char word_buf[256];
        int wpos = 0;
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = reply[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        // Собираем подряд '1'
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
// Рабочая функция потока: он отправляет map<...> всем своим чанкам
// -------------------------------
static void *map_worker_thread(void *arg) {
    WorkerTask *task = (WorkerTask *)arg;
    int my_index = task->thread_index;
    int step = task->total_workers;

    // Открываем ZMQ_REQ-сокет к данному воркеру
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

    // Перебираем все чанки, которые должен обработать этот поток:
    // (my_index, my_index+step, my_index+2*step, ...)
    for (size_t i = my_index; i < g_num_chunks; i += step) {
        // Формируем "map" + chunk
        char msg[MSG_SIZE];
        memset(msg, 0, sizeof(msg));
        snprintf(msg, sizeof(msg), "map%s", g_chunks[i]);

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

    while (p != NULL) {
        const char *w = p->word;
        int wlen = (int)strlen(w);

        // Если само слово не влезает, придётся пропустить (иначе зациклится)
        if (pos + wlen >= outsize - 1) {
            // Чтобы не уйти в бесконечный цикл, удалим этот элемент вообще:
            // (хотя формально мы теряем данные, но иначе застрянем)
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

        // Копируем слово
        memcpy(out + pos, w, wlen);
        pos += wlen;

        // Добавляем '1' count раз (пока влезает)
        while (p->count > 0 && pos < outsize - 1) {
            out[pos++] = '1';
            p->count--;
        }

        // Если мы исчерпали count, удаляем из списка
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
            // иначе идём дальше
            prev = p;
            p = p->next;
        }

        // Если уже места не хватает хотя бы на 1 символ — выходим
        if (pos >= outsize - 1) {
            break;
        }
    }
    pthread_mutex_unlock(&g_lock);
}

// -------------------------------
// Добавляет (word, c) в g_final_list
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

// -------------------------------
// Парсит REDUCE ответ: "the2example2..."
// -------------------------------
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
        }
    }
}

// -------------------------------
// Сортируем FinalPair: убывание по count, при равенстве — алфавит
// -------------------------------
static int cmpfunc(const void *a, const void *b) {
    const FinalPair *fa = *(const FinalPair **)a;
    const FinalPair *fb = *(const FinalPair **)b;
    if (fa->count > fb->count) return -1;
    if (fa->count < fb->count) return +1;
    return strcmp(fa->word, fb->word);
}

// -------------------------------
// Функция разбивки filecontent на чанки по ~1496 символов
// с попыткой не резать слово
// -------------------------------
static void add_chunk(char *start, size_t len) {
    // Увеличиваем емкость массива при необходимости
    if (g_num_chunks == g_chunks_capacity) {
        size_t newcap = (g_chunks_capacity == 0) ? 256 : g_chunks_capacity * 2;
        g_chunks = realloc(g_chunks, newcap * sizeof(*g_chunks));
        g_chunks_capacity = newcap;
    }
    g_chunks[g_num_chunks] = strndup(start, len);
    g_num_chunks++;
}

static void split_into_chunks(char *filecontent, size_t chunk_size) {
    char *ptr = filecontent;
    while (*ptr) {
        size_t len = strlen(ptr);
        if (len == 0) {
            break;
        }
        size_t actual_chunk_size = (len > chunk_size) ? chunk_size : len;

        // Попытка не резать слово, если не конец текста
        if (actual_chunk_size < len && ptr[actual_chunk_size] != ' ') {
            size_t tmp = actual_chunk_size;
            // Ищем пробел назад
            while (tmp > 0 && ptr[tmp - 1] != ' ') {
                tmp--;
            }
            // Если не нашли пробел, tmp станет 0
            // В этом случае режем слово (берём chunk_size)
            if (tmp > 0) {
                actual_chunk_size = tmp;
            }
        }
        // Добавляем новый чанк
        add_chunk(ptr, actual_chunk_size);

        // Сдвигаем ptr
        ptr += actual_chunk_size;
        // Пропускаем пробелы
        while (*ptr == ' ') {
            ptr++;
        }
    }
}

// -----------------------------------------------------------------------------
// MAIN Distributor
// -----------------------------------------------------------------------------
int main(int argc, char *argv[])
{
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

    // Читаем входной файл
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

    // Нарезаем на чанки (примерно 1496 символов)
    split_into_chunks(filecontent, 1496);

    // Запускаем по одному потоку на воркер: каждый обрабатывает свои чанки
    pthread_t *threads = malloc(num_workers * sizeof(pthread_t));
    WorkerTask *tasks = malloc(num_workers * sizeof(WorkerTask));

    for (int i = 0; i < num_workers; i++) {
        tasks[i].worker_index = i;
        tasks[i].endpoint = endpoints[i];
        tasks[i].thread_index = i;
        tasks[i].total_workers = num_workers;
        pthread_create(&threads[i], NULL, map_worker_thread, &tasks[i]);
    }

    // Ждём все map-потоки
    for (int i = 0; i < num_workers; i++) {
        pthread_join(threads[i], NULL);
    }

    // --- reduce: используем endpoints[0] ---
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

    // Цикл, пока g_map_results не опустеет
    while (1) {
        pthread_mutex_lock(&g_lock);
        if (g_map_results == NULL) {
            pthread_mutex_unlock(&g_lock);
            break; // всё обработано
        }
        pthread_mutex_unlock(&g_lock);

        int rcvtimeo = 2000; // 2 секунды
        zmq_setsockopt(reduce_socket, ZMQ_RCVTIMEO, &rcvtimeo, sizeof(rcvtimeo));

        char reduce_msg[MSG_SIZE];
        build_reduce_payload(reduce_msg, MSG_SIZE);

        if (zmq_send(reduce_socket, reduce_msg, strlen(reduce_msg), 0) == -1) {
            perror("zmq_send reduce");
            break;
        }

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

    // Сортируем и печатаем результат
    // Считаем сколько элементов
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

    // Печатаем CSV
    printf("word,frequency\n");
    for (int i = 0; i < count_final; i++) {
        printf("%s,%d\n", arr[i]->word, arr[i]->count);
    }
    free(arr);

    // "rip" всем воркерам
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
                    // ожидаем "rip"
                }
            }
            zmq_close(s);
        }
    }

    zmq_ctx_destroy(g_zmq_context);

    // Очистка
    for (int i = 0; i < num_workers; i++) {
        free(endpoints[i]);
    }
    free(endpoints);
    free(tasks);
    free(threads);

    // Освобождаем память чанков
    for (size_t i = 0; i < g_num_chunks; i++) {
        free(g_chunks[i]);
    }
    free(g_chunks);

    free(filecontent);

    return 0;
}
