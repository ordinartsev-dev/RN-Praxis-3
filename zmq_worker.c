/*************************************************************
 *  zmq_worker.c (Перероблено з використанням Ordered HashMap
 *  для підрахунку слів)
 *
 *  Логіка воркера:
 *   - Запускається командою: ./zmq_worker <port1> [<port2> ...]
 *   - Привʼязується (bind) до сокета типу REP на кожному з
 *     переданих портів.
 *   - Приймає повідомлення з командами "map", "red" або "rip".
 *   - Для "map" і "red" виконує обробку даних за допомогою
 *     впорядкованого хеш-словника (Ordered HashMap) з
 *     підрахунком слів і збереженням порядку вставки, а потім
 *     формує рядок-відповідь.
 *   - Для "rip" відправляє "rip" і завершує свою роботу.
 *************************************************************/

#include <stdio.h>    // Бібліотека вводу-виводу (printf, perror, тощо)
#include <stdlib.h>   // Загальні функції (malloc, free, atoi, тощо)
#include <string.h>   // Робота з рядками (strcpy, strcmp, strtok, тощо)
#include <ctype.h>    // Функції для перевірки й перетворення символів (isalpha, tolower)
#include <zmq.h>      // Бібліотека ZeroMQ (обмін повідомленнями)
#include <unistd.h>   // Функції системи UNIX (close, sleep, тощо)

#define MAX_MSG_SIZE 1500  // Максимальний розмір повідомлення (у байтах)
#define HASH_SIZE 1024      // Кількість бакетів у нашому хеш-словнику

/*************************************************************
 *   ВПОРЯДКОВАНИЙ ХЕШ- СЛОВНИК (Ordered HashMap)
 *************************************************************/

/*
 * Кожен вузол (HashNode) зберігає:
 *  - key: слово (рядок)
 *  - count: частота (підрахунок) слова
 *  - next: вказівник на наступний вузол у поточному бакеті
 *  - order_next: вказівник на наступний вузол у
 *    ланцюжку порядку вставки
 */
typedef struct HashNode {
    char *key;                 // Слово
    int count;                 // Частота зустрічання
    struct HashNode *next;     // Вказівник на наступний у тому ж бакеті
    struct HashNode *order_next; // Вказівник на наступний за порядком вставки
} HashNode;

/*
 * Структура HashMap зберігає:
 *  - buckets: масив вказівників на бакети (розмір = HASH_SIZE)
 *  - order_head: початок списку за порядком вставки
 *  - order_tail: кінець списку за порядком вставки
 */
typedef struct HashMap {
    HashNode **buckets;        // Масив із HASH_SIZE бакетів
    HashNode *order_head;      // Початок ланцюжка порядку вставки
    HashNode *order_tail;      // Кінець ланцюжка порядку вставки
} HashMap;

/*
 * hash_function: Алгоритм djb2 для хешування рядка.
 * Приймає const char* (рядок), повертає ціле значення.
 * Результат береться за модулем HASH_SIZE, щоб вийшов індекс бакета.
 */
unsigned int hash_function(const char *str) {
    unsigned long hash = 5381;
    int c;
    // Перебираємо кожен символ у рядку і коригуємо хеш
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return (unsigned int)(hash % HASH_SIZE);
}

/*
 * create_hashmap: Виділяє памʼять для HashMap,
 * ініціалізує масив бакетів і вказівники order_head/tail.
 */
HashMap *create_hashmap(void) {
    HashMap *map = malloc(sizeof(HashMap));
    if (!map) return NULL;  // Якщо malloc повернув NULL, завершуємо
    map->buckets = calloc(HASH_SIZE, sizeof(HashNode *));
    map->order_head = NULL;
    map->order_tail = NULL;
    return map;
}

/*
 * hm_insert: вставляє слово (key) з певним count у хеш-словник.
 * Якщо слово вже є, збільшує його лічильник.
 * Якщо слова немає, створює новий вузол і додає його в бакет
 * і в кінець ланцюга вставки.
 */
void hm_insert(HashMap *map, const char *key, int count) {
    // Обчислюємо індекс бакета
    unsigned int index = hash_function(key);
    HashNode *node = map->buckets[index];

    // Перевіряємо, чи існує слово вже
    while (node) {
        if (strcmp(node->key, key) == 0) {
            // Якщо знайшли, збільшуємо лічильник
            node->count += count;
            return;
        }
        node = node->next;
    }
    // Якщо слово не знайдено, створюємо новий вузол
    HashNode *new_node = malloc(sizeof(HashNode));
    new_node->key = strdup(key); // Копіюємо рядок key
    new_node->count = count;
    // Вставляємо на початок бакета
    new_node->next = map->buckets[index];
    map->buckets[index] = new_node;
    // Спочатку встановлюємо order_next у NULL
    new_node->order_next = NULL;
    // Додаємо у кінець ланцюжка вставки
    if (map->order_tail) {
        map->order_tail->order_next = new_node;
        map->order_tail = new_node;
    } else {
        map->order_head = new_node;
        map->order_tail = new_node;
    }
}

/*
 * free_hashmap: видаляє всі вузли й звільняє памʼять,
 * виділену для хеш-словника.
 */
void free_hashmap(HashMap *map) {
    if (!map) return;
    // Ітеруємося за order_head -> order_next
    HashNode *node = map->order_head;
    while (node) {
        HashNode *tmp = node;
        node = node->order_next;
        free(tmp->key);  // Звільняємо рядок
        free(tmp);       // Звільняємо сам вузол
    }
    free(map->buckets); // Звільняємо масив бакетів
    free(map);          // Звільняємо саму структуру мапи
}

/*************************************************************
 *  ЛОГІКА ВОРКЕРА
 *************************************************************/

/*
 * map_function:
 *  - Приймає рядок (payload), де можуть бути різні символи.
 *  - Перетворює будь-які неалфавітні символи у пробіли.
 *  - Переводить усі літери в нижній регістр (tolower).
 *  - Розбиває на слова (strtok), для кожного слова додає "1"
 *    у лічильник в нашій тимчасовій HashMap (збережена послідовність).
 *  - Потім проходить по порядку вставки (order_head -> order_tail),
 *    формує вихідний рядок: "word111..."
 *    (записує слово + стільки '1', скільки count).
 *  - Повертає результат як C-рядок.
 */
static char *map_function(const char *payload) {
    // Копіюємо payload, щоб його змінювати (strdup)
    char *copy = strdup(payload);
    // Замінюємо все, що не букви, на пробіли + робимо букви нижнього регістра
    for (size_t i = 0; i < strlen(copy); i++) {
        if (!isalpha((unsigned char)copy[i]))
            copy[i] = ' ';
        else
            copy[i] = (char)tolower((unsigned char)copy[i]);
    }

    // Створюємо тимчасовий хеш-словник
    HashMap *map = create_hashmap();
    // Розбиваємо copy на токени (слова)
    char *token = strtok(copy, " \t\r\n");
    while (token) {
        hm_insert(map, token, 1); // Кожне слово +1
        token = strtok(NULL, " \t\r\n");
    }

    // Формуємо результат у статичному буфері result
    static char result[MAX_MSG_SIZE];
    memset(result, 0, sizeof(result));
    int idx = 0;
    HashNode *curr = map->order_head;
    // Ідемо за порядком вставки
    while (curr) {
        int key_len = (int)strlen(curr->key);
        // Перевіряємо, чи вистачить місця
        if (idx + key_len >= MAX_MSG_SIZE - 1)
            break;
        // Копіюємо слово
        memcpy(result + idx, curr->key, key_len);
        idx += key_len;
        // Додаємо count разів '1'
        for (int j = 0; j < curr->count; j++) {
            if (idx >= MAX_MSG_SIZE - 1)
                break;
            result[idx++] = '1';
        }
        curr = curr->order_next;
    }
    // Страхуємо, щоб рядок завершувався '\0'
    result[MAX_MSG_SIZE - 1] = '\0';

    free(copy);
    free_hashmap(map);
    return result;
}

/*
 * reduce_function:
 *  - Приймає рядок формату "word111word111..." (де '1'
 *    відображають кількість).
 *  - Утворює HashMap (з порядком вставки).
 *  - Розбирає слово (букви) + рахує кількість '1'
 *    (наприклад, якщо 2 '1', то count=2).
 *  - Потім створює результат: "word2word2..." (наприклад),
 *    де число після слова вказує суму '1'.
 */
static char *reduce_function(const char *payload) {
    // Створюємо тимчасовий хеш-словник
    HashMap *map = create_hashmap();
    int i = 0;
    int n = (int)strlen(payload);
    while (i < n) {
        char word_buffer[256];
        int word_pos = 0;
        // Збираємо послідовність літер
        while (i < n && isalpha((unsigned char)payload[i])) {
            if (word_pos < 255)
                word_buffer[word_pos++] = payload[i];
            i++;
        }
        word_buffer[word_pos] = '\0';

        // Лічимо '1'
        int count = 0;
        while (i < n && payload[i] == '1') {
            count++;
            i++;
        }

        // Якщо є слово + кількість, вставляємо в map
        if (word_pos > 0 && count > 0) {
            hm_insert(map, word_buffer, count);
        }
    }

    // Будуємо відповідь у статичному буфері
    static char result[MAX_MSG_SIZE];
    memset(result, 0, sizeof(result));
    int pos = 0;
    HashNode *curr = map->order_head;
    while (curr) {
        int key_len = (int)strlen(curr->key);
        if (pos + key_len >= MAX_MSG_SIZE - 1)
            break;
        // Копіюємо слово
        memcpy(result + pos, curr->key, key_len);
        pos += key_len;
        // Додаємо число (count)
        char nbuffer[64];
        snprintf(nbuffer, sizeof(nbuffer), "%d", curr->count);
        int num_len = (int)strlen(nbuffer);
        if (pos + num_len >= MAX_MSG_SIZE - 1)
            break;
        memcpy(result + pos, nbuffer, num_len);
        pos += num_len;
        curr = curr->order_next;
    }
    // Закінчуємо рядок
    result[MAX_MSG_SIZE - 1] = '\0';

    free_hashmap(map);
    return result;
}

/*************************************************************
 *  ГОЛОВНА ФУНКЦІЯ (MAIN) для ZeroMQ Worker
 *************************************************************/
int main(int argc, char *argv[]) {
    // Перевірка аргументів: мусить бути принаймні 2
    // (назва програми + 1 порт)
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }

    // Створюємо контекст ZeroMQ
    void *cont = zmq_ctx_new();
    if (!cont) {
        perror("zmq_ctx_new");
        return 1;
    }

    // Створюємо сокет REP (запит-відповідь)
    void *rep_sock = zmq_socket(cont, ZMQ_REP);
    if (!rep_sock) {
        perror("zmq_socket");
        zmq_ctx_destroy(cont);
        return 1;
    }

    // Встановлюємо опцію LINGER=0, щоб сокет закривався миттєво
    int linger = 0;
    zmq_setsockopt(rep_sock, ZMQ_LINGER, &linger, sizeof(linger));

    // За бажанням, ставимо таймаут отримання (RCVTIMEO)
    int rcvtime = 1000; // мс
    zmq_setsockopt(rep_sock, ZMQ_RCVTIMEO, &rcvtime, sizeof(rcvtime));

    // Привʼязуємо сокет REP до кожного порта, переданого в argv
    for (int i = 1; i < argc; i++) {
        char end[128];
        snprintf(end, sizeof(end), "tcp://*:%s", argv[i]);
        if (zmq_bind(rep_sock, end) != 0) {
            perror("zmq_bind");
        } else {
            printf("Worker bound to %s\n", end);
            //fflush(stdout); // За бажанням
        }
    }

    /*
     * Основний цикл:
     *  - Чекає на повідомлення (zmq_recv).
     *  - Перевіряє перші 3 символи, щоб визначити команду
     *    (map / red / rip).
     *  - Викликає відповідну функцію (map_function або reduce_function)
     *    або завершує при rip.
     */
    while (1) {
        char buffer[MAX_MSG_SIZE];
        memset(buffer, 0, sizeof(buffer));
        int recv_size = zmq_recv(rep_sock, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            // Якщо таймаут або помилка, просто продовжуємо
            perror("zmq_recv");
            continue;
        }
        // Закінчуємо отриманий рядок '\0'
        buffer[recv_size] = '\0';

        // Генеруємо простий ключ із перших трьох символів (наприклад, "map")
        int command_key = (buffer[0] << 16) | (buffer[1] << 8) | buffer[2];

        // Відділяємо payload (рядок після перших 3 символів)
        const char *payload = buffer + 3;

        char reply[MAX_MSG_SIZE];
        memset(reply, 0, sizeof(reply));

        if (command_key == ('m' << 16 | 'a' << 8 | 'p')) {
            // "map"
            char *res = map_function(payload);
            strncpy(reply, res, MAX_MSG_SIZE - 1);
            reply[MAX_MSG_SIZE - 1] = '\0';
            zmq_send(rep_sock, reply, strlen(reply) + 1, 0);
        }
        else if (command_key == ('r' << 16 | 'e' << 8 | 'd')) {
            // "red"
            char *res = reduce_function(payload);
            strncpy(reply, res, MAX_MSG_SIZE - 1);
            reply[MAX_MSG_SIZE - 1] = '\0';
            zmq_send(rep_sock, reply, strlen(reply) + 1, 0);
        }
        else if (command_key == ('r' << 16 | 'i' << 8 | 'p')) {
            // "rip": завершуємо
            strcpy(reply, "rip");
            reply[MAX_MSG_SIZE - 1] = '\0';
            zmq_send(rep_sock, reply, strlen(reply) + 1, 0);
            printf("Worker received rip -> exiting\n");
            fflush(stdout);
            break;
        }
        else {
            // Невідома команда: шлемо порожню відповідь
            zmq_send(rep_sock, "", 0, 0);
        }
    }

    // Закриваємо сокет та контекст
    zmq_close(rep_sock);
    zmq_ctx_destroy(cont);
    printf("Worker done.\n");
    return 0;
}
