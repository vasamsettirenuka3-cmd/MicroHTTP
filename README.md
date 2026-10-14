# MICROHTTP

An educational concurrent static HTTP server written in C for Ubuntu Linux.

## Features
- TCP server on port 8080
- Four POSIX worker threads
- Bounded client queue protected by a mutex
- Condition variables for queue coordination
- GET requests and static files from www/
- Persistent connections, limited to 100 requests
- Request-header timeout
- Dynamic request-buffer allocation and cleanup
- SIGINT/SIGTERM shutdown
- Rejection of traversal paths and symlinks

## Build and run
make
make run

Run commands from the MicroHTTP project directory.

## Test from another terminal
curl -i http://localhost:8080/
curl -i http://localhost:8080/missing.html
curl -i -X POST http://localhost:8080/
curl --path-as-is -i http://localhost:8080/../src/server.c

Expected status codes: 200, 404, 405, 403.

## Memory checking
make asan
ASAN_OPTIONS=detect_leaks=1 ./microhttp_asan

Run the requests from another terminal, then stop the server.
Inspect sanitizer output for memory errors and leaks.

## Windows browser access with WSL2
Keep the server running.
Try http://localhost:8080/ in Windows Chrome.
If needed, run hostname -I in Ubuntu and use:
http://YOUR_WSL_IP:8080/

The WSL IP can change after restarting WSL.

## Operating-system concepts
CO-1: Socket and file system calls; user-space server using kernel services.
CO-2: Server process lifecycle; explicit fork/exec/wait is not implemented.
CO-3: Signals and socket communication; pipes/FIFOs are not implemented.
CO-4: Heap allocation with malloc/free and memory debugging.
CO-5: File descriptors, openat, read, fstat, close, and inode inspection.
CO-6: POSIX threads, mutexes, condition variables, and a bounded queue.

## Limitations
- Educational server; not a complete HTTP/1.1 implementation.
- GET only; request bodies are unsupported.
- No TLS, authentication, or dynamic application backend.
- mmap, semaphores, and copy-on-write demonstrations are not implemented.
- Passing tests does not prove that all concurrency or protocol errors are absent.
