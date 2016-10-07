/*
 * Simple CLI interface to Hoover
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> /* gethostname */
#include <string.h>

#include "hooverio.h"
#include "hooverrmq.h"

int main(int argc, char **argv) {
    struct hoover_comm_config *config;
    struct hoover_tube *tube;

    /* Load the tube configuration  */
    if ( !(config = read_comm_config()) ) {
        fprintf( stderr, "NULL config\n" );
        return 1;
    }
    else {
        save_comm_config( config, stdout );
    }

    /* Set up the tube (AMQP connection, socket, exchange, and channel) */
    if ( (tube = create_hoover_tube(config)) == NULL ) {
        fprintf( stderr, "could not establish tube\n" );
        return 1;
    }

    /* Load file into an hoover data object */
    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <file name> [file name [file name [...]]]\n", argv[0] );
        return 1;
    }

    char **filenames = &argv[1];
    uint32_t num_files = argc - 1;
    uint32_t num_headers = 0;
    struct hoover_header **headers = malloc(sizeof(struct hoover_header) * num_files);
    memset(headers, 0, sizeof(struct hoover_header) * num_files);
    if ( !headers ) {
        fprintf( stderr, "couldn't allocate memory for headers\n" );
        return 1;
    }
    for ( uint32_t i = 0; i < num_files; i++ ) {
        FILE *fp;
        if ( !(fp = fopen(filenames[i], "r")) ) {
            fprintf( stderr, "could not open file %s\n", filenames[i] );
            continue;
        }

        /* Load file in as an HDO */
        struct hoover_data_obj *hdo = hoover_create_hdo(fp, HOOVER_BLK_SIZE);
        fclose(fp);
        if ( !hdo ) {
            fprintf( stderr, "got NULL HDO from %s\n", filenames[i] );
            continue;
        }

        /* Build header for HDO */
        struct hoover_header *header = build_hoover_header( filenames[i], hdo );
        if ( !header ) {
            fprintf( stderr, "got NULL header from %s\n", filenames[i] );
            free_hdo( hdo );
            continue;
        }

        /* Send the message */
        printf("Sending %s\n", filenames[i]);
        hoover_send_message( tube, hdo, header );

        /* Release the HDO, but retain the header to build the manifest */
        free_hdo(hdo);
        headers[num_headers++] = header;
    }

    /* build the manifest */
    char *manifest = build_manifest(headers, num_files);
    /* turn manifest into HDO */
    struct hoover_data_obj *manifest_hdo = manifest_to_hdo(manifest, strlen(manifest));
    /* create manifest header - first figure out what it should be called */
    char *manifest_fn_template = "manifest_%s_%s.json";
    size_t manifest_fn_len = sizeof(char)*(strlen(manifest_fn_template) + HOST_NAME_MAX + SHA_DIGEST_LENGTH_HEX + 1);
    char *manifest_fn = malloc(manifest_fn_len);
    if (!manifest_fn ) {
        fprintf(stderr, "unable to allocate memory for manifest file name\n" );
        /* cleanup rather than return */
        return 1;
    }
    else {
        char hostname[HOST_NAME_MAX];
        gethostname(hostname, HOST_NAME_MAX);
        snprintf(manifest_fn, manifest_fn_len, manifest_fn_template, manifest_hdo->hash, hostname);
    }
    /* then build the manifest HDO's header */
    struct hoover_header *manifest_header = build_hoover_header(manifest_fn, manifest_hdo);

    /* send the manifest HDO as the final piece */
    hoover_send_message( tube, manifest_hdo, manifest_header );

    /* tear down everything */
    free(manifest_fn);
    free_hoover_header(manifest_header);
    free_hdo(manifest_hdo);
    for (uint32_t i = 0; i < num_files; i++)
        free_hoover_header(headers[i]);
    free(headers);

    /* tear down communication structures */
    free_hoover_tube(tube);
    free_comm_config(config);

    return 0;
}
