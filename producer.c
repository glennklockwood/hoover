/*
 * Simple CLI interface to Hoover
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "hooverio.h"
#include "hooverrmq.h"

int main(int argc, char **argv) {
    FILE *fp;
    int status;

    struct hoover_comm_config *config;
    struct hoover_header *header;
    struct hoover_data_obj *hdo;
    struct hoover_tube *tube;
    hoover_buffer message;

    srand(time(NULL) ^ getpid());

    /*
     *  Initialize a bunch of nonsense
     */
    if ( !(config = read_comm_config()) ) {
        fprintf( stderr, "NULL config\n" );
        return 1;
    }
    else {
        save_comm_config( config, stdout );
    }
    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <file name>\n", argv[0] );
        return 1;
    }

    /*
     *  Set up the AMQP connection, socket, exchange, and channel
     */
    if ( (tube = create_hoover_tube(config)) == NULL ) {
        fprintf( stderr, "could not establish tube\n" );
        return 1;
    }

    /*
     *  Begin handling data here
     */
    /* Load file into hoover data object */
    fp = fopen( argv[1], "r" );
    if ( !fp ) {
        fprintf( stderr, "could not open file %s\n", argv[1] );
        return 1;
    }
    hdo = hoover_load_file( fp, HOOVER_BLK_SIZE );

    /* Build header */
    header = build_hoover_header( argv[1], hdo );
    if ( !header ) {
        fprintf( stderr, "got null header\n" );
        free_hdo( hdo );
        return 1;
    }

    /* Send the message */
    message.len = hdo->size;
    message.bytes = hdo->data;
    hoover_send_message( 
        tube,
        &message,
        config->exchange,
        config->routing_key,
        header );

    /* Tear down everything */
    free_hoover_header(header);
    free_hdo(hdo);

    free_hoover_tube(tube);

    return 0;
}
