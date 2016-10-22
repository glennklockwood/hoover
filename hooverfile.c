/*******************************************************************************
 *  hooverrmq.c
 *
 *  File-based tube interface to Hoover.
 *
 *  Glenn K. Lockwood, Lawrence Berkeley National Laboratory       October 2016
 ******************************************************************************/
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 700
    #define _XOPEN_SOURCE 700
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <libgen.h>

#include "hooverio.h"
#include "hooverfile.h"

struct hoover_tube_config *read_tube_config(void) {
    struct hoover_tube_config *config;
    config = calloc(1, sizeof(struct hoover_tube_config));
    getcwd(config->dir, PATH_MAX);
    return config;
}
void save_tube_config(struct hoover_tube_config *config, FILE *out) {
    return;
}
void free_tube_config(struct hoover_tube_config *config) {
    if ( config == NULL ) {
        fprintf( stderr, "free_tube_config: received NULL pointer\n" );
        return;
    }
    free(config);
    return;
}

/*
 * Convert a bunch of runtime structures into an AMQP message and send it
 */
void hoover_send_message( struct hoover_tube *tube,
                          struct hoover_data_obj *hdo,
                          struct hoover_header *header ) {
    char buf[PATH_MAX] = "";
    char *bn;
    FILE *fp;

    strncat(buf, tube->dir, PATH_MAX);
    strncat(buf, "/", PATH_MAX);
    strncat(buf, header->filename, PATH_MAX);
    bn = basename(buf);

    fp = fopen( bn, "w" );
    if ( !fp ) {
        fprintf(stderr, "hoover_send_message: could not open %s for writing\n", bn);
        return;
    }
    else {
        fprintf(stderr, "hoover_send_message: writing %s\n", bn);
    }
        
    hoover_write_hdo( fp, hdo, 512*1024 );
    fclose(fp);
    return;
}

struct hoover_tube *create_hoover_tube(struct hoover_tube_config *config) {
    struct hoover_tube *tube;
    tube = calloc(1, sizeof(struct hoover_tube));
    strncpy(tube->dir, config->dir, PATH_MAX);
    return tube;
}

void free_hoover_tube( struct hoover_tube *tube ) {
    if ( tube == NULL ) {
        fprintf( stderr, "free_hoover_tube: received NULL pointer\n" );
        return;
    }
    free(tube);
    return;
}
