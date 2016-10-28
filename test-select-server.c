#include <stdio.h>
#include <stdlib.h>
#include <string.h>

 /* for srand line */
#include <unistd.h>
#include <time.h>

#include "hooverrmq.h"

extern char *select_server(struct hoover_tube_config *config);

int main(int argc, char **argv) {
    struct hoover_tube_config *a;
    a = malloc(sizeof(*a));

    srand(time(NULL) ^ getpid());

    if ( argc < 2 ) {
        fprintf(stderr, "Syntax: %s <server1> [server 2 [...]]\n", argv[0]);
        return 1;
    }
    else {
        printf( "Got %d servers\n", argc - 1 );
    }

    a->max_hosts = argc-1;
    a->remaining_hosts = a->max_hosts;
    for (uint32_t i = 0; i < a->max_hosts; i++) {
        a->servers[i] = malloc(256*sizeof(char));
        strncpy(a->servers[i], argv[1+i], 256);
        printf("Loaded [%s]\n", a->servers[i]);
    }

    for (uint32_t i = 0; i < a->max_hosts; i++) {
        printf( "%s\n", select_server( a ) );
    }

    return 0;
}
