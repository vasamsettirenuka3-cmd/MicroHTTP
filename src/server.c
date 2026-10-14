/*
 * MicroHTTP - Concurrent HTTP/1.1 Web Server
 *
 * Features:
 * - TCP socket server
 * - Thread pool
 * - Thread-safe client queue
 * - HTTP GET
 * - Static file serving
 * - 200 / 400 / 403 / 404 / 408 responses
 * - Content-Type
 * - Content-Length
 * - Directory traversal protection
 * - HTTP keep-alive
 * - Connection timeout
 * - Graceful SIGINT/SIGTERM shutdown
 * - SO_REUSEADDR
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <poll.h>
#include <time.h>

#define PORT 8080

#define WORKER_COUNT 4
#define QUEUE_SIZE 32

#define BUFFER_SIZE 8192
#define FILE_BUFFER_SIZE 8192

/* Idle timeout for a client */
#define CLIENT_TIMEOUT 10

/* Maximum requests on one keep-alive connection */
#define MAX_KEEP_ALIVE_REQUESTS 100


/* =========================================================
   GLOBAL SHUTDOWN FLAG
   ========================================================= */

_Static_assert(ATOMIC_INT_LOCK_FREE == 2, "Lock-free atomics required");
atomic_int server_running = 1;


/* =========================================================
   CLIENT QUEUE
   ========================================================= */

typedef struct
{
    int clients[QUEUE_SIZE];

    int front;
    int rear;
    int count;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;

} client_queue_t;

client_queue_t client_queue;


/* =========================================================
   SIGNAL HANDLER
   ========================================================= */

void handle_signal(int signal_number)
{
    (void)signal_number;

    /*

     * Signal handler only changes a flag.
     * It does not call printf(), malloc(), etc.
     */
    server_running = 0;
}


/* =========================================================
   SEND ALL
   ========================================================= */

int send_all(int fd, const char *buffer, size_t length)
{
    size_t total = 0;

    while (total < length)
    {
        ssize_t sent = send(
            fd,
            buffer + total,
            length - total,
            MSG_NOSIGNAL
        );

        if (sent < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        if (sent == 0)
            return -1;

        total += sent;
    }

    return 0;
}


/* =========================================================
   SEND ERROR RESPONSE
   ========================================================= */

void send_error(
    int fd,
    const char *status,


    const char *message
)
{
    char response[1024];

    size_t message_length = strlen(message);

    int length = snprintf(
        response,
        sizeof(response),

        "HTTP/1.1 %s\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n"
        "%s",

        status,
        message_length,
        message
    );

    if (length > 0 && (size_t)length < sizeof(response))
        send_all(fd, response, (size_t)length);
}


/* =========================================================
   CONTENT TYPE
   ========================================================= */

const char *get_content_type(const char *path)
{
    const char *extension =
        strrchr(path, '.');

    if (extension == NULL)
        return "application/octet-stream";

    if (strcmp(extension, ".html") == 0 ||
        strcmp(extension, ".htm") == 0)
        return "text/html";

    if (strcmp(extension, ".css") == 0)
        return "text/css";

    if (strcmp(extension, ".js") == 0)
        return "application/javascript";

    if (strcmp(extension, ".json") == 0)
        return "application/json";

    if (strcmp(extension, ".txt") == 0)
        return "text/plain";

    if (strcmp(extension, ".png") == 0)
        return "image/png";

    if (strcmp(extension, ".jpg") == 0 ||
        strcmp(extension, ".jpeg") == 0)
        return "image/jpeg";

    if (strcmp(extension, ".gif") == 0)
        return "image/gif";

    return "application/octet-stream";
}


/* =========================================================
   READ ONE HTTP REQUEST
   ========================================================= */

/* Read one header block without consuming the next pipelined request. */
int read_http_request(int fd, char *request, size_t request_size)
{
    size_t total = 0;
    struct timespec begin, now;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    while (total < request_size - 1 && server_running)
    {
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - begin.tv_sec >= CLIENT_TIMEOUT)
        { errno = EAGAIN; return -1; }
        struct pollfd ready = { .fd = fd, .events = POLLIN };
        int result = poll(&ready, 1, 100);
        if (result < 0) { if (errno == EINTR) continue; return -1; }
        if (result == 0) continue;
        ssize_t n = recv(fd, request + total, 1, 0);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) break;
        if (request[total] == '\0') { errno = EINVAL; return -1; }
        request[++total] = '\0';
        if (total >= 4 && memcmp(request + total - 4, "\r\n\r\n", 4) == 0)
            return (int)total;
    }
    request[total] = '\0';
    return (int)total;
}

/* =========================================================
   SEND STATIC FILE
   ========================================================= */

int serve_file(
    int client_fd,
    const char *path,
    int keep_alive
)
{
    char file_path[2048];

    snprintf(
        file_path,
        sizeof(file_path),
        "www%s",
        path
    );

    printf(
        "Opening file: %s\n",
        file_path
    );


    /* Open file */

    /* Walk beneath www, refusing symlinks in every path component. */
    int file_fd = -1;
    int directory = open("www", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    char parts[1024];
    snprintf(parts, sizeof(parts), "%s", path + 1);
    char *state = NULL;
    char *part = strtok_r(parts, "/", &state);
    while (directory >= 0 && part != NULL)
    {
        char *next = strtok_r(NULL, "/", &state);
        int opened = openat(directory, part,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK | (next ? O_DIRECTORY : 0));
        close(directory);
        directory = -1;
        if (next) directory = opened;
        else file_fd = opened;
        part = next;
    }
    if (directory >= 0) close(directory);

    if (file_fd < 0)
    {
        printf(
            "File not found: %s\n",
            file_path
        );

        send_error(
            client_fd,
            "404 Not Found",
            "404 Not Found\n"
        );

        return -1;
    }


    /* File information */

    struct stat file_info;

    if (fstat(
            file_fd,
            &file_info
        ) < 0)
    {
        close(file_fd);

        send_error(
            client_fd,
            "500 Internal Server Error",
            "Internal Server Error\n"
        );

        return -1;
    }


    /* Only regular files */

    if (!S_ISREG(file_info.st_mode))
    {
        close(file_fd);

        send_error(
            client_fd,
            "403 Forbidden",
            "403 Forbidden\n"
        );

        return -1;
    }


    off_t file_size =
        file_info.st_size;


    const char *content_type =
        get_content_type(path);


    /* HTTP response header */

    char header[1024];

    int header_length = snprintf(
        header,
        sizeof(header),

        "HTTP/1.1 200 OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %ld\r\n"
        "Connection: %s\r\n"
        "\r\n",

        content_type,
        (long)file_size,

        keep_alive
            ? "keep-alive"
            : "close"
    );


    if (header_length < 0 || (size_t)header_length >= sizeof(header))
    { close(file_fd); return -1; }

    if (send_all(
            client_fd,
            header,
            header_length
        ) < 0)
    {
        close(file_fd);
        return -1;
    }


    /* Send file */

    char buffer[FILE_BUFFER_SIZE];

    ssize_t bytes_read;

    while ((bytes_read = read(
        file_fd,
        buffer,
        sizeof(buffer)
    )) > 0)
    {
        if (send_all(
                client_fd,
                buffer,
                bytes_read
            ) < 0)
        {
            close(file_fd);
            return -1;
        }
    }


    if (bytes_read < 0) { close(file_fd); return -1; }
    close(file_fd);

    printf(
        "File sent successfully: %s\n",
        path
    );

    return 0;
}


/* =========================================================
   HANDLE CLIENT
   ========================================================= */


void handle_client(int client_fd)
{
    struct timeval timeout = {
        .tv_sec = CLIENT_TIMEOUT,
        .tv_usec = 0
    };

    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO,
                   &timeout, sizeof(timeout)) < 0 ||
        setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO,
                   &timeout, sizeof(timeout)) < 0)
    {
        perror("setsockopt client timeout");
        close(client_fd);
        return;
    }

    /* CO-4: one heap buffer per client connection. */
    char *request = malloc(BUFFER_SIZE);

    if (request == NULL)
    {
        send_error(client_fd,
                   "500 Internal Server Error",
                   "Memory allocation failed\n");
        close(client_fd);
        return;
    }

    int request_count = 0;

    while (server_running &&
           request_count < MAX_KEEP_ALIVE_REQUESTS)
    {
        int bytes = read_http_request(
            client_fd, request, BUFFER_SIZE
        );

        if (bytes == 0)
            break;

        if (bytes < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                send_error(client_fd,
                           "408 Request Timeout",
                           "Request Timeout\n");
            }
            else
            {
                perror("recv");
            }
            break;
        }

        if (strstr(request, "\r\n\r\n") == NULL)
        {
            send_error(client_fd,
                       "400 Bad Request",
                       "Incomplete or oversized request headers\n");
            break;
        }

        char lower[BUFFER_SIZE];
        for (int i = 0; i <= bytes; i++)
        {
            unsigned char c = (unsigned char)request[i];
            lower[i] = (c >= 'A' && c <= 'Z') ? (char)(c + 'a' - 'A') : (char)c;
        }
        if (strstr(lower, "\r\ncontent-length:") ||
            strstr(lower, "\r\ntransfer-encoding:"))
        {
            send_error(client_fd, "400 Bad Request", "Request bodies unsupported\n");
            break;
        }
        request_count++;

        char method[16];
        char path[1024];
        char version[16];

        if (sscanf(request, "%15s %1023s %15s",
                   method, path, version) != 3)
        {
            send_error(client_fd,
                       "400 Bad Request",
                       "Bad Request\n");
            break;
        }

        if (strcmp(method, "GET") != 0)
        {
            send_error(client_fd,
                       "405 Method Not Allowed",
                       "Method Not Allowed\n");
            break;
        }

        if (strcmp(version, "HTTP/1.1") != 0 &&
            strcmp(version, "HTTP/1.0") != 0)
        {
            send_error(client_fd,
                       "400 Bad Request",
                       "Unsupported HTTP version\n");
            break;
        }

        if (path[0] != '/')
        {
            send_error(client_fd,
                       "400 Bad Request",
                       "Bad Request\n");
            break;
        }

        /* Remove the query string before looking up the file. */
        char *query = strchr(path, '?');
        if (query != NULL)
            *query = '\0';

        if (strstr(path, "..") != NULL)
        {
            send_error(client_fd,
                       "403 Forbidden",
                       "403 Forbidden\n");
            break;
        }

        if (strcmp(path, "/") == 0)
            strcpy(path, "/index.html");

        int wants_close = 0, wants_alive = 0;
        char *line = strstr(lower, "\r\n");
        while (line && line[2] != '\r')
        {
            line += 2;
            char *end = strstr(line, "\r\n");
            if (!end) break;
            *end = '\0';
            if (strncmp(line, "connection:", 11) == 0)
            {
                char *value = line + 11, *next;
                do {
                    next = strchr(value, ',');
                    if (next) *next = '\0';
                    while (*value == ' ' || *value == '\t') value++;
                    char *tail = value + strlen(value);
                    while (tail > value && (tail[-1] == ' ' || tail[-1] == '\t')) *--tail = '\0';
                    if (strcmp(value, "close") == 0) wants_close = 1;
                    if (strcmp(value, "keep-alive") == 0) wants_alive = 1;
                    value = next ? next + 1 : NULL;
                } while (value);
            }
            line = end;
        }
        int keep_alive = !wants_close &&
            (strcmp(version, "HTTP/1.1") == 0 || wants_alive);

        if (!server_running ||
            request_count >= MAX_KEEP_ALIVE_REQUESTS)
        {
            keep_alive = 0;
        }

        printf("Client FD %d - Request #%d: %s %s\n",
               client_fd, request_count, method, path);

        if (serve_file(client_fd, path, keep_alive) < 0)
            break;

        if (!keep_alive)
            break;
    }

    /* CO-4: all loop exit paths release the allocated buffer. */
    free(request);
    close(client_fd);

    printf("Client FD %d closed\n", client_fd);
}

/* Queue initialization */
void queue_init(client_queue_t *queue)
{
    queue->front = 0;
    queue->rear = 0;
    queue->count = 0;

    pthread_mutex_init(
        &queue->mutex,
        NULL
    );

    pthread_cond_init(
        &queue->not_empty,
        NULL
    );

    pthread_cond_init(
        &queue->not_full,
        NULL
    );
}


/* =========================================================
   QUEUE PUSH
   ========================================================= */

void queue_push(
    client_queue_t *queue,
    int client_fd
)
{
    pthread_mutex_lock(
        &queue->mutex
    );


    while (
        queue->count == QUEUE_SIZE &&
        server_running
    )
    {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec++;
        pthread_cond_timedwait(&queue->not_full, &queue->mutex, &deadline);
    }


    if (!server_running)
    {
        pthread_mutex_unlock(
            &queue->mutex
        );

        close(client_fd);

        return;
    }


    queue->clients[queue->rear] =
        client_fd;

    queue->rear =
        (queue->rear + 1) % QUEUE_SIZE;

    queue->count++;


    pthread_cond_signal(
        &queue->not_empty
    );


    pthread_mutex_unlock(
        &queue->mutex
    );
}


/* =========================================================
   QUEUE POP
   ========================================================= */

int queue_pop(
    client_queue_t *queue
)
{
    pthread_mutex_lock(
        &queue->mutex
    );


    while (
        queue->count == 0 &&
        server_running
    )
    {
        pthread_cond_wait(
            &queue->not_empty,
            &queue->mutex
        );
    }


    if (
        queue->count == 0 &&
        !server_running
    )
    {
        pthread_mutex_unlock(
            &queue->mutex
        );

        return -1;
    }


    int client_fd =
        queue->clients[queue->front];


    queue->front =
        (queue->front + 1) % QUEUE_SIZE;

    queue->count--;


    pthread_cond_signal(
        &queue->not_full
    );


    pthread_mutex_unlock(
        &queue->mutex
    );


    return client_fd;
}


/* =========================================================
   WORKER THREAD
   ========================================================= */

void *worker_thread(void *argument)
{
    int worker_id =
        *(int *)argument;


    printf(
        "Worker %d started\n",
        worker_id
    );


    while (server_running)
    {
        int client_fd =
            queue_pop(
                &client_queue
            );


        if (client_fd < 0)
            break;


        printf(
            "Worker %d handling FD %d\n",
            worker_id,
            client_fd
        );


        handle_client(
            client_fd
        );
    }


    printf(
        "Worker %d stopped\n",
        worker_id
    );


    return NULL;
}


/* =========================================================
   MAIN
   ========================================================= */

int main(void)
{
    /* =============================================
       INSTALL SIGNAL HANDLERS
       ============================================= */

    struct sigaction sa;

    memset(
        &sa,
        0,
        sizeof(sa)
    );

    sa.sa_handler =
        handle_signal;

    sigemptyset(
        &sa.sa_mask
    );

    sa.sa_flags = 0;


    if (sigaction(
            SIGINT,
            &sa,
            NULL
        ) < 0)
    {
        perror("sigaction SIGINT");
        return 1;
    }


    if (sigaction(
            SIGTERM,
            &sa,
            NULL
        ) < 0)
    {
        perror("sigaction SIGTERM");
        return 1;
    }


    /*
     * Prevent process termination when a client
     * closes its socket unexpectedly.
     */

    signal(
        SIGPIPE,
        SIG_IGN
    );


    /* =============================================
       CREATE SERVER SOCKET
       ============================================= */

    int server_fd =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (server_fd < 0)
    {
        perror("socket");
        return 1;
    }


    /* =============================================
       SO_REUSEADDR
       ============================================= */

    int reuse = 1;


    if (setsockopt(
            server_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            sizeof(reuse)
        ) < 0)
    {
        perror("setsockopt");

        close(server_fd);

        return 1;
    }


    /* =============================================
       SERVER ADDRESS
       ============================================= */

    struct sockaddr_in server_addr;

    memset(
        &server_addr,
        0,
        sizeof(server_addr)
    );


    server_addr.sin_family =
        AF_INET;

    server_addr.sin_addr.s_addr =
        INADDR_ANY;

    server_addr.sin_port =
        htons(PORT);


    /* =============================================
       BIND
       ============================================= */

    if (bind(
            server_fd,
            (struct sockaddr *)&server_addr,
            sizeof(server_addr)
        ) < 0)
    {
        perror("bind");

        close(server_fd);

        return 1;
    }


    /* =============================================
       LISTEN
       ============================================= */

    if (listen(
            server_fd,
            QUEUE_SIZE
        ) < 0)
    {
        perror("listen");

        close(server_fd);

        return 1;
    }


    /* =============================================
       INITIALIZE QUEUE
       ============================================= */

    queue_init(
        &client_queue
    );


    /* =============================================
       CREATE WORKERS
       ============================================= */

    pthread_t workers[
        WORKER_COUNT
    ];

    int worker_ids[
        WORKER_COUNT
    ];


    printf("\n");
    printf("=================================\n");
    printf("      MicroHTTP Server\n");
    printf("=================================\n");
    printf("Port: %d\n", PORT);
    printf("Workers: %d\n", WORKER_COUNT);
    printf("Queue size: %d\n", QUEUE_SIZE);
    printf("Client timeout: %d seconds\n", CLIENT_TIMEOUT);
    printf("Keep-alive: ENABLED\n");
    printf("Graceful shutdown: ENABLED\n");
    printf("=================================\n\n");


    int created_workers = 0;
    int exit_status = 0;
    for (int i = 0; i < WORKER_COUNT; i++)
    {
        worker_ids[i] = i + 1;


        int error = pthread_create(&workers[i], NULL, worker_thread, &worker_ids[i]);
        if (error != 0)
        {
            fprintf(stderr, "pthread_create: %s\n", strerror(error));
            server_running = 0;
            exit_status = 1;
            break;
        }
        created_workers++;
    }

    printf(
        "Server is ready.\n"
    );

    printf(
        "Waiting for clients...\n\n"
    );


    /* =============================================
       ACCEPT LOOP
       ============================================= */

    int flags = fcntl(server_fd, F_GETFL, 0);
    if (flags < 0 || fcntl(server_fd, F_SETFL, flags | O_NONBLOCK) < 0)
    { perror("fcntl"); server_running = 0; exit_status = 1; }
    while (server_running)
    {
        struct pollfd listener = { .fd = server_fd, .events = POLLIN };
        int ready = poll(&listener, 1, 100);
        if (ready < 0) { if (errno == EINTR) continue; perror("poll"); exit_status = 1; break; }
        if (ready == 0) continue;
        int client_fd =
            accept(
                server_fd,
                NULL,
                NULL
            );


        if (client_fd < 0)
        {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
            {
                /*
                 * SIGINT/SIGTERM interrupted accept().
                 */
                continue;
            }

            perror("accept");

            continue;
        }


        if (!server_running)
        {
            close(client_fd);
            break;
        }


        printf(
            "New client connected: FD %d\n",
            client_fd
        );


        queue_push(
            &client_queue,
            client_fd
        );
    }


    /* =============================================
       GRACEFUL SHUTDOWN
       ============================================= */

    printf("\n");
    printf("=================================\n");
    printf("Graceful shutdown started...\n");
    printf("Stopping new connections...\n");
    printf("Stopping worker threads...\n");
    printf("=================================\n");


    server_running = 0;

    /*
     * Wake workers waiting on queue.
     */

    pthread_mutex_lock(
        &client_queue.mutex
    );


    pthread_cond_broadcast(
        &client_queue.not_empty
    );


    pthread_cond_broadcast(
        &client_queue.not_full
    );


    pthread_mutex_unlock(
        &client_queue.mutex
    );


    /*
     * Close listening socket.
     */

    close(server_fd);


    /*
     * Wait for workers.
     */

    for (int i = 0; i < created_workers; i++)
    {
        pthread_join(
            workers[i],
            NULL
        );
    }


    /* =============================================
       DESTROY SYNCHRONIZATION OBJECTS
       ============================================= */

    while (client_queue.count > 0)
    {
        close(client_queue.clients[client_queue.front]);
        client_queue.front = (client_queue.front + 1) % QUEUE_SIZE;
        client_queue.count--;
    }

    pthread_mutex_destroy(
        &client_queue.mutex
    );

    pthread_cond_destroy(
        &client_queue.not_empty
    );

    pthread_cond_destroy(
        &client_queue.not_full
    );


    printf(
        "\nMicroHTTP stopped cleanly.\n"
    );


    return exit_status;
}


