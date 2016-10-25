/*
 * Test the process of generating the manifest and, by extension, HDO headers
 */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h> /* for hdo stuff */

#include "hooverio.h"

/* from hooverfile.o */
extern size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size );

int main(int argc, char **argv) {
    FILE *fp;

    struct hoover_tube_config *config;
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
        headers[i] = build_hoover_header( argv[i+1], hdo, "" );
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

    /* Now convert manifest into an HDO */
    FILE *fp_out = fopen( "manifest.json.gz", "w" );
    hdo = manifest_to_hdo( manifest, strlen(manifest) );
    if ( hdo != NULL ) {
        printf( "Loaded:        %ld bytes\n", hdo->size_orig );
        printf( "Original hash: %s\n",        hdo->hash_orig );
        printf( "Saving:        %ld bytes\n", hdo->size );
        printf( "Saved hash:    %s\n",        hdo->hash );
        if (fp_out) hoover_write_hdo( fp_out, hdo, HOOVER_BLK_SIZE );
        free_hdo( hdo );
    }
    else {
        fprintf(stderr, "hoover_create_hdo failed (errno=%d)\n", errno );
        if (fp_out) fclose(fp_out);
        return 1;
    }
    if (fp_out) fclose(fp_out);

    return 0;
}
