/*
 * Simple CLI interface to Hoover
 */
#include <stdio.h>
#include <stdlib.h>

#include "hooverio.h"
#include "hooverrmq.h"

int main(int argc, char **argv) {
    FILE *fp;

    struct hoover_comm_config *config;
    struct hoover_header *header;
    struct hoover_data_obj *hdo;
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
        fprintf( stderr, "Syntax: %s <file name>\n", argv[0] );
        return 1;
    }
    if ( !(fp = fopen( argv[1], "r" )) ) {
        fprintf( stderr, "could not open file %s\n", argv[1] );
        return 1;
    }
    hdo = hoover_create_hdo( fp, HOOVER_BLK_SIZE );

    /* Build header */
    header = build_hoover_header( argv[1], hdo );
    if ( !header ) {
        fprintf( stderr, "got null header\n" );
        free_hdo( hdo );
        return 1;
    }

    /* Send the message */
    hoover_send_message( tube, hdo, header );

    /* Tear down everything */
    free_hoover_header(header);
    free_hdo(hdo);
    free_hoover_tube(tube);
    free_comm_config(config);

    return 0;
}
