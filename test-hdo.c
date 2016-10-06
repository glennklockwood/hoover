/*
 * Test block encoding components of HDO generation
 */
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include "hooverio.h"

int main( int argc, char **argv )
{
    FILE *fp_in, *fp_out;
    struct stat st;
    struct hoover_data_obj *hdo;

    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <input file> [output file]\n", argv[0] );
        return 1;
    }
    else if ( argc < 3 )
        fp_out = NULL;
    else
        fp_out = fopen( argv[2], "w" );
    fp_in = fopen( argv[1], "r" );

    if ( !fp_in ) {
        fprintf( stderr, "Could not open input file %s\n", argv[1] );
        return ENOENT;
    }

    hdo = hoover_create_hdo( fp_in, HOOVER_BLK_SIZE );

    fclose(fp_in);

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
