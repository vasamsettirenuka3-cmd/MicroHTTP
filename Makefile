.RECIPEPREFIX := >
CC = gcc
CFLAGS = -std=gnu11 -Wall -Wextra -Wpedantic -g
LDLIBS = -pthread

.PHONY: all run asan clean

all: microhttp

microhttp: src/server.c
>$(CC) $(CFLAGS) src/server.c -o $@ $(LDLIBS)

run: microhttp
>./microhttp

asan: src/server.c
>$(CC) $(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer src/server.c -o microhttp_asan $(LDLIBS)

clean:
>rm -f microhttp microhttp_asan
