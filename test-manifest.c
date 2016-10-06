/*
 * Test the process of generating the manifest and, by extension, HDO headers
 */
#include <stdio.h>
#include <stdlib.h>

#include "hooverio.h"

int main(int argc, char **argv) {
    FILE *fp;

    struct hoover_comm_config *config;
    struct hoover_header *header;
    struct hoover_data_obj *hdo;
    struct hoover_tube *tube;

    struct hoover_header **headers;
    int num_files;
    int i;

    char *manifest;

    /* Load file into an hoover data object */
    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <file name> [file name [file name [...]]]\n", argv[0] );
        return 1;
    }

    num_files = argc - 1;
    headers = malloc(sizeof(*headers) * num_files);
    for ( i = 0; i < num_files; i++ ) {
        if ( !(fp = fopen( argv[i+1], "r" )) ) {
            fprintf( stderr, "could not open file %s\n", argv[i+1] );
            return 1;
        }

        hdo = hoover_create_hdo( fp, HOOVER_BLK_SIZE );

        /* Build header */
        headers[i] = build_hoover_header( argv[i+1], hdo );
        if ( !headers[i] ) {
            fprintf( stderr, "got null header\n" );
            return 1;
        }

        free_hdo(hdo);
        fclose(fp);
    }

    manifest = build_manifest( headers, num_files );
    printf( "%s\n", manifest );

    /* Tear down everything */
    for ( i = 0; i < num_files; i++ )
        free(headers[i]);
    free(headers);

    return 0;
}
