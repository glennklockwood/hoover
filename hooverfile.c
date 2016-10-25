/*******************************************************************************
 *  hooverfile.c
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

/*******************************************************************************
 * Private functions
 ******************************************************************************/

/**
 * Write a memory buffer to a file block by block
 */
size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size ) {
    void *p_out = hdo->data;
    size_t bytes_written,
           tot_bytes_written = 0,
           bytes_left = hdo->size;
    do {
        if ( bytes_left > block_size )
            bytes_written = fwrite( p_out, 1, block_size, fp );
        else
            bytes_written = fwrite( p_out, 1, bytes_left, fp );
        p_out = (char*)p_out + bytes_written;
        tot_bytes_written += bytes_written;
        bytes_left = hdo->size - tot_bytes_written;
    } while ( bytes_left != 0 );

    return tot_bytes_written;
}

/*******************************************************************************
 * Global functions
 ******************************************************************************/
/**
 *  Load file output configuration parameters
 */
struct hoover_tube_config *read_tube_config(void) {
    struct hoover_tube_config *config;
    config = calloc(1, sizeof(struct hoover_tube_config));
    getcwd(config->dir, PATH_MAX);
    return config;
}

/**
 *  Save RabbitMQ configuration parameters in some serialized format.  Ideally
 *  the output of this function can serve as the input of the read_tube_config()
 *  function.
 */
void save_tube_config(struct hoover_tube_config *config, FILE *out) {
    fprintf( out, "%s\n", config->dir );
    return;
}

/**
 * Destroy hoover_tube_config and free all strings
 */
void free_tube_config(struct hoover_tube_config *config) {
    if ( config == NULL ) {
        fprintf( stderr, "free_tube_config: received NULL pointer\n" );
        return;
    }
    free(config);
    return;
}

/**
 *  Create a tube and get to a state where it can be used to send HDOs
 */
struct hoover_tube *create_hoover_tube(struct hoover_tube_config *config) {
    struct hoover_tube *tube;
    tube = calloc(1, sizeof(struct hoover_tube));
    strncpy(tube->dir, config->dir, PATH_MAX);
    return tube;
}

/**
 * Destroy a hoover tube and all substructures.
 */
void free_hoover_tube( struct hoover_tube *tube ) {
    if ( tube == NULL ) {
        fprintf( stderr, "free_hoover_tube: received NULL pointer\n" );
        return;
    }
    free(tube);
    return;
}

/**
 * Convert Hoover structures into a file
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
