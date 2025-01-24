/*************************************************************
 *  zmq_worker.c
 *  Логика Воркера:
 *   - запускается как ./zmq_worker <port1> [<port2> ...]
 *   - на каждом порту делает REP-сокет
 *   - принимает "map...", "red...", "rip"
 *   - при "map" / "red" обрабатывает, отвечает
 *   - при "rip" отвечает "rip" и завершает
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <zmq.h>
#include <unistd.h>

#define MSG_SIZE 1500

// Структура WordCount для map/reduce
typedef struct WordCount {
    char *word;
    int count;
    struct WordCount *next;
} WordCount;

// Добавляет (w, c) в конец списка (head, tail),
// чтобы сохранить порядок появления
static void add_word_tail(WordCount **head, WordCount **tail, const char *w, int c) {
    // Проверяем, есть ли уже
    WordCount *cur = *head;
    while (cur) {
        if (strcmp(cur->word, w) == 0) {
            cur->count += c;
            return;
        }
        cur = cur->next;
    }
    // Создаём новый узел
    WordCount *newwc = malloc(sizeof(*newwc));
    newwc->word = strdup(w);
    newwc->count = c;
    newwc->next = NULL;

    if (*head == NULL) {
        *head = newwc;
        *tail = newwc;
    } else {
        (*tail)->next = newwc;
        *tail = newwc;
    }
}

// -----------------------------------------------------------------------------
// map_function: разбирает payload -> "the11example11..."
// -----------------------------------------------------------------------------
static char *map_function(const char *payload) {
    char *copy = strdup(payload);
    // Всё не-буквенное -> ' ', tolower
    for (size_t i = 0; i < strlen(copy); i++) {
        if (!isalpha((unsigned char)copy[i])) {
            copy[i] = ' ';
        } else {
            copy[i] = (char)tolower((unsigned char)copy[i]);
        }
    }

    WordCount *head = NULL;
    WordCount *tail = NULL;

    char *token = strtok(copy, " \t\r\n");
    while (token) {
        add_word_tail(&head, &tail, token, 1);
        token = strtok(NULL, " \t\r\n");
    }

    static char result[MSG_SIZE];
    memset(result, 0, sizeof(result));
    int pos = 0;

    // Собираем "word + '1'*count" по порядку
    WordCount *p = head;
    while (p) {
        int wlen = (int)strlen(p->word);
        // Проверка места
        if (pos + wlen >= MSG_SIZE - 1) {
            break;
        }
        memcpy(result + pos, p->word, wlen);
        pos += wlen;

        // Добавляем '1' count раз (пока влезает)
        for (int i = 0; i < p->count; i++) {
            if (pos >= MSG_SIZE - 1) {
                break;
            }
            result[pos++] = '1';
        }
        p = p->next;
    }

    // Очистка
    while (head) {
        WordCount *tmp = head;
        head = head->next;
        free(tmp->word);
        free(tmp);
    }
    free(copy);

    return result;
}

// -----------------------------------------------------------------------------
// reduce_function: "the11example11..." -> "the2example2..."
// Аналогично, сохраняем порядок слов как в payload
// -----------------------------------------------------------------------------
static char *reduce_function(const char *payload) {
    WordCount *head = NULL;
    WordCount *tail = NULL;

    int i = 0;
    int n = (int)strlen(payload);

    while (i < n) {
        // слово
        char word_buf[256];
        int wpos = 0;
        while (i < n && isalpha((unsigned char)payload[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = payload[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        // подряд '1'
        int count = 0;
        while (i < n && payload[i] == '1') {
            count++;
            i++;
        }

        if (wpos > 0 && count > 0) {
            add_word_tail(&head, &tail, word_buf, count);
        }
    }

    static char result[MSG_SIZE];
    memset(result, 0, sizeof(result));
    int pos = 0;

    // Собираем "word + число" в том же порядке
    WordCount *p = head;
    while (p) {
        int wlen = (int)strlen(p->word);
        if (pos + wlen >= MSG_SIZE - 1) {
            break;
        }
        memcpy(result + pos, p->word, wlen);
        pos += wlen;

        char numbuf[32];
        snprintf(numbuf, sizeof(numbuf), "%d", p->count);
        int numlen = (int)strlen(numbuf);
        if (pos + numlen >= MSG_SIZE - 1) {
            break;
        }
        memcpy(result + pos, numbuf, numlen);
        pos += numlen;

        p = p->next;
    }

    // очистка
    while (head) {
        WordCount *tmp = head;
        head = head->next;
        free(tmp->word);
        free(tmp);
    }

    return result;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }

    // Создаём ZMQ-контекст
    void *context = zmq_ctx_new();
    if (!context) {
        perror("zmq_ctx_new");
        return 1;
    }

    // REP‑сокет, биндим на все порты
    void *responder = zmq_socket(context, ZMQ_REP);
    if (!responder) {
        perror("zmq_socket");
        zmq_ctx_destroy(context);
        return 1;
    }

    int linger = 0;
    zmq_setsockopt(responder, ZMQ_LINGER, &linger, sizeof(linger));

    int rcvtime = 2000; // 2с
    zmq_setsockopt(responder, ZMQ_RCVTIMEO, &rcvtime, sizeof(rcvtime));

    for (int i = 1; i < argc; i++) {
        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://*:%s", argv[i]);
        if (zmq_bind(responder, endpoint) != 0) {
            perror("zmq_bind");
        } else {
            printf("Worker bind to %s\n", endpoint);
            fflush(stdout);
        }
    }

    // Цикл
    while (1) {
        char buffer[MSG_SIZE];
        memset(buffer, 0, sizeof(buffer));
        int recv_size = zmq_recv(responder, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            perror("zmq_recv");
            continue;
        }
        buffer[recv_size] = '\0';

        // первые 3 символа — тип
        char type[4];
        memcpy(type, buffer, 3);
        type[3] = '\0';

        const char *payload = buffer + 3;

        char reply[MSG_SIZE];
        memset(reply, 0, sizeof(reply));

        if (strcmp(type, "map") == 0) {
            char *res = map_function(payload);
            strncpy(reply, res, MSG_SIZE - 1);
            zmq_send(responder, reply, strlen(reply), 0);
        }
        else if (strcmp(type, "red") == 0) {
            char *res = reduce_function(payload);
            strncpy(reply, res, MSG_SIZE - 1);
            zmq_send(responder, reply, strlen(reply), 0);
        }
        else if (strcmp(type, "rip") == 0) {
            strcpy(reply, "rip");
            zmq_send(responder, reply, strlen(reply), 0);
            printf("Worker received rip -> exiting\n");
            fflush(stdout);
            break;
        }
        else {
            // неизвестный тип
            zmq_send(responder, "", 0, 0);
        }
    }

    // Закрываем
    zmq_close(responder);
    zmq_ctx_destroy(context);
    printf("Worker done.\n");
    return 0;
}
