/*******************************************************************************
 *  hooverrmq.c
 *
 *  RabbitMQ interface to Hoover.  Portions of this code is derived from
 *  rabbitmq_slurmdlog.c in CSG/nersc-slurm.git, written by D. M. Jacobsen.
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

#include "hooverio.h"
#include "hooverrmq.h"

/*******************************************************************************
 *  Private functions
 ******************************************************************************/
static int parse_amqp_response(amqp_rpc_reply_t x, char const *context, int die);
static char *trim(char *string);
static char *select_server(struct hoover_tube_config *config);
static amqp_table_t *create_amqp_header_table( struct hoover_header *header );
static void free_amqp_header_table( amqp_table_t *table );

/**
 *  Randomly select a server from the list of servers, then pop it off the list
 */
char *select_server(struct hoover_tube_config *config) {
    if (config->remaining_hosts == 0 || config->max_hosts == 0) return NULL;

    size_t idx;
    char *server, *tmp;

    idx = rand() % config->max_hosts;
    server = config->servers[idx];

    /* swap last element with selected element */
    tmp = config->servers[idx];
    config->servers[idx] = config->servers[config->remaining_hosts - 1];
    config->servers[config->remaining_hosts - 1] = tmp;

    /* shorten the candidate list so we don't try the same server twice */
    config->remaining_hosts--;
    return server;
}


/**
 * Strip leading/trailing whitespace from a string
 */
static char *trim(char *string) {
    if (string == NULL || strlen(string) == 0)
        return string;

    char *left = string;
    char *right = string + strlen(string) - 1;

    while (left && *left && isspace(*left))
        left++;
    while (right > left && right && *right && isspace(*right))
        right--;
    right++;
    *right = '\0';
    return left;
}

/**
 * Check return of a rabbitmq-c call and throw an error + clean up if it is a
 * failure
 */
static int parse_amqp_response(amqp_rpc_reply_t x, char const *context, int die) {
    amqp_connection_close_t *conn_close_reply;
    amqp_channel_close_t *chan_close_reply;
    switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return 0;

    case AMQP_RESPONSE_NONE:
        fprintf(stderr, "%s: missing RPC reply type!\n", context);
        break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
        break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
            case AMQP_CONNECTION_CLOSE_METHOD:
                conn_close_reply = (amqp_connection_close_t *) x.reply.decoded;
                fprintf(stderr, "%s: server connection error %d, message: %.*s\n",
                    context,
                    conn_close_reply->reply_code,
                    (int)conn_close_reply->reply_text.len, (char *)conn_close_reply->reply_text.bytes);
                break;
            case AMQP_CHANNEL_CLOSE_METHOD:
                chan_close_reply = (amqp_channel_close_t *) x.reply.decoded;
                fprintf(stderr, "%s: server channel error %d, message: %.*s\n",
                    context,
                    chan_close_reply->reply_code,
                    (int)chan_close_reply->reply_text.len, (char *)chan_close_reply->reply_text.bytes);
                break;
            default:
                fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                break;
        }
        break;
    }
    if ( die )
        exit(1);
    else
        return 1;
}

/**
 *  Convert a hoover_header into an AMQP table to be attached to a message
 */
#define HOOVER_HEADER_ENTRIES 6 /* number of elements in struct hoover_header */
static amqp_table_t *create_amqp_header_table( struct hoover_header *header ) {
    amqp_table_t *table;
    amqp_table_entry_t *entries;

    if ( !(table = malloc(sizeof(*table))) )
        return NULL;
    if ( !(entries = malloc(HOOVER_HEADER_ENTRIES * sizeof(*entries))) ) {
        free(table);
        return NULL;
    }

    table->num_entries = HOOVER_HEADER_ENTRIES;

    /* Set headers */
    entries[0].key = amqp_cstring_bytes("filename");
    entries[0].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[0].value.value.bytes = amqp_cstring_bytes(header->filename);

    entries[1].key = amqp_cstring_bytes("node_id");
    entries[1].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[1].value.value.bytes = amqp_cstring_bytes(header->node_id);

    entries[2].key = amqp_cstring_bytes("task_id");
    entries[2].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[2].value.value.bytes = amqp_cstring_bytes(header->task_id);

    entries[3].key = amqp_cstring_bytes("compression");
    entries[3].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[3].value.value.bytes = amqp_cstring_bytes(header->compression);

    entries[4].key = amqp_cstring_bytes("sha_hash");
    entries[4].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[4].value.value.bytes = amqp_cstring_bytes((char*)header->sha_hash);

    entries[5].key = amqp_cstring_bytes("size");
    entries[5].value.kind = AMQP_FIELD_KIND_I64;
    entries[5].value.value.i64 = header->size;

    table->entries = entries;

    return table;
}

/**
 * Destroy amqp_table_t and all entries
 */
static void free_amqp_header_table( amqp_table_t *table ) {
    free(table->entries);
    free(table);
    return;
}

/*******************************************************************************
 * Global functions
 ******************************************************************************/

/**
 *  Load RabbitMQ configuration parameters
 */
struct hoover_tube_config *read_tube_config(void) {
    FILE *fp = fopen(HOOVER_CONFIG_FILE, "r");
    if (fp == NULL) return NULL;

    struct hoover_tube_config *config = (struct hoover_tube_config *) malloc(sizeof(struct hoover_tube_config));
    if ( !config ) return NULL;

    memset(config, 0, sizeof(struct hoover_tube_config));
    
    char *p = NULL;
    size_t ps = 0;

    /* required by select_server */
    srand(time(NULL) ^ getpid());

    while (!feof(fp) && !ferror(fp)) {
        size_t nread = getline(&p, &ps, fp);
        if (nread == 0 || feof(fp) || ferror(fp)) {
            break;
        }
        char *key = p;
        char *ptr = strchr(key, '=');
        if (ptr == NULL) continue;
        *ptr = 0;
        
        char *value = ptr + 1;
        key = trim(key);
        value = trim(value);

        if (strcmp(key, "servers") == 0) {
            char *save_ptr = NULL;
            char *t_value = NULL;
            char *search = value;

            while ((t_value = strtok_r(search, ",", &save_ptr)) != NULL) {
                search = NULL;
                t_value = trim(t_value);
                if (t_value == NULL) continue;

                if ( config->max_hosts < HOOVER_MAX_SERVERS ) {
                    config->servers[config->max_hosts] = strdup(t_value);
                    printf( "Found server %s\n", config->servers[config->max_hosts] );
                    config->max_hosts++;
                }
                else {
                    fprintf( stderr, "too many servers in config file; truncating at %d\n", HOOVER_MAX_SERVERS );
                    break;
                }
            }
            config->remaining_hosts = config->max_hosts;
        } else if (strcmp(key, "port") == 0) {
            config->port = atoi(value);
        } else if (strcmp(key, "vhost") == 0) {
            config->vhost = strdup(value);
        } else if (strcmp(key, "username") == 0) {
            config->username = strdup(value);
        } else if (strcmp(key, "password") == 0) {
            config->password = strdup(value);
        } else if (strcmp(key, "exchange") == 0) {
            config->exchange = strdup(value);
        } else if (strcmp(key, "exchangeType") == 0 || strcmp(key, "exchange_type") == 0) {
            config->exchange_type = strdup(value);
        } else if (strcmp(key, "queue") == 0) {
            config->queue = strdup(value);
        } else if (strcmp(key, "routingKey") == 0 || strcmp(key, "routing_key") == 0) {
            config->routing_key = strdup(value);
        } else if (strcmp(key, "maxTransmitSize") == 0 || strcmp(key, "max_transmit_size") == 0) {
            config->max_transmit_size = strtoul(value, NULL, 10);
        } else if (strcmp(key, "use_ssl") == 0) {
            config->use_ssl = atoi(value);
        }
    }
    free(p);
    return config;
}

/**
 *  Save RabbitMQ configuration parameters in some serialized format.  Ideally
 *  the output of this function can serve as the input of the read_tube_config()
 *  function.
 */
void save_tube_config(struct hoover_tube_config *config, FILE *out) {
    if (config == NULL || out == NULL) return;

    int i;
    fprintf(out, "servers: ");
    for ( i = 0 ; i < config->max_hosts; i++) {
        fprintf(out, "%s%s", (i == 0 ? "" : ", "), config->servers[i] );
    }
    fprintf(out, "\nport: %d\n", config->port);
    fprintf(out, "vhost: %s\n", config->vhost);
    fprintf(out, "username: %s\n", config->username);
    fprintf(out, "password: %s\n", config->password);
    fprintf(out, "exchange: %s\n", config->exchange);
    fprintf(out, "exchange_type: %s\n", config->exchange_type);
    fprintf(out, "queue: %s\n", config->queue);
    fprintf(out, "routing_key: %s\n", config->routing_key);
    fprintf(out, "max_transmit_size: %lu\n", config->max_transmit_size);
    fprintf(out, "use_ssl: %d\n", config->use_ssl);

    return;
}

/**
 * Destroy hoover_tube_config and free all strings
 *
 * Note that you MUST destroy all tubes that rely on a config before freeing
 * that config with this function.  Tubes alias the 'exchange' and 'routing_key'
 * members which are freed here.
 */
void free_tube_config( struct hoover_tube_config *config ) {
    int i;
    if (config == NULL) {
        fprintf( stderr, "free_tube_config: received NULL pointer\n" );
        return;
    }

    for (i = 0; i < config->max_hosts; i++)
        if (config->servers[i] != NULL)
            free(config->servers[i]);

    if (config->vhost         != NULL) free(config->vhost);
    if (config->username      != NULL) free(config->username);
    if (config->password      != NULL) free(config->password);
    if (config->exchange      != NULL) free(config->exchange);    /* note that hoover_tube aliases this string */
    if (config->exchange_type != NULL) free(config->exchange_type);
    if (config->queue         != NULL) free(config->queue);
    if (config->routing_key   != NULL) free(config->routing_key); /* note that hoover_tube aliases this string */

    free(config);
    return;
}

/**
 *  Create a tube and get to a state where it can be used to send HDOs
 */
struct hoover_tube *create_hoover_tube(struct hoover_tube_config *config) {
    int connected, status;
    char *hostname;
    struct hoover_tube *tube;
    amqp_rpc_reply_t reply;

    if (!(tube = malloc(sizeof(*tube))))
        return NULL;
    memset(tube, 0, sizeof(*tube));

    /* establish socket */
    for (connected = 0, hostname = select_server(config);
                        hostname != NULL;
                        hostname = select_server(config) ) {
        printf( "Attempting to connect to %s:%d\n", hostname, config->port );
        tube->connection = amqp_new_connection();

        if ( config->use_ssl )
            tube->socket = amqp_ssl_socket_new(tube->connection);
        else
            tube->socket = amqp_tcp_socket_new(tube->connection);

        if (tube->socket == NULL) {
            fprintf(stderr, "Failed to create socket!\n");
            free_hoover_tube(tube);
            return NULL;
        }

        if ( config->use_ssl ) {
            amqp_ssl_socket_set_verify_peer(tube->socket, 0);
            amqp_ssl_socket_set_verify_hostname(tube->socket, 0);
        }

        status = amqp_socket_open(tube->socket, hostname, config->port);

        if (status != 0) {
            fprintf( stderr, "Failed to connect to %s:%d; moving on...\n", hostname, config->port );
        }
        else {
            connected = 1;
            break;
        }
    }

    /* make sure connection exists */
    if (!connected) {
        fprintf(stderr, "Failed to connect to any servers!\n");
        free_hoover_tube(tube);
        return NULL;
    }

    /* authenticate */
    reply = amqp_login(
        tube->connection,       /* amqp_connection_state_t state */
        config->vhost,          /* char const *vhost */
        0,                      /* int channel_max */
        131072,                 /* int frame_max */
        0,                      /* int heartbeat */
        AMQP_SASL_METHOD_PLAIN, /* amqp_sasl_method_enum sasl_method */
        config->username,
        config->password);

    if ( parse_amqp_response(reply, "login", false) ) {
        free_hoover_tube(tube);
        return NULL;
    }

    /* open channel */
    tube->channel = 1;
    amqp_channel_open(tube->connection, tube->channel);
    if ( parse_amqp_response(amqp_get_rpc_reply(tube->connection), "channel open", false) ) {
        free_hoover_tube(tube);
        return NULL;
    }

    /* exchange/routing_key required to send messages, so include them in the
     * tube */
    tube->exchange = amqp_cstring_bytes(config->exchange);
    tube->routing_key = amqp_cstring_bytes(config->routing_key);

    amqp_exchange_declare(
        tube->connection,                         /* amqp_connection_state_t state */
        tube->channel,                            /* amqp_channel_t channel */
        tube->exchange,                           /* amqp_bytes_t exchange */
        amqp_cstring_bytes(config->exchange_type),/* amqp_bytes_t type */
        0,                                        /* amqp_boolean_t passive */
        0,                                        /* amqp_boolean_t durable */
        0,                                        /* amqp_boolean_t auto_delete */
        0,                                        /* amqp_boolean_t internal */
        amqp_empty_table                          /* amqp_table_t arguments */
    );
    if ( parse_amqp_response(amqp_get_rpc_reply(tube->connection), "exchange declare", false) ) {
        free_hoover_tube(tube);
        return NULL;
    }

    return tube;
}

/**
 * Destroy a hoover tube and all substructures.
 *
 * Note that semantics are a little different in that this function not only
 * frees memory structures, but it also safely tears down communication with the
 * RabbitMQ broker.
 */
void free_hoover_tube( struct hoover_tube *tube ) {
    if ( tube == NULL ) {
        fprintf( stderr, "free_hoover_tube: received NULL pointer\n" );
        return;
    }
    /* Closes all channels, notifies broker of shutdown, closes the socket, then
     * destroys the connection */
    if ( tube->connection ) {
        parse_amqp_response(amqp_connection_close(tube->connection, AMQP_REPLY_SUCCESS), "connection close", false);
        amqp_destroy_connection(tube->connection);
    }

    free(tube);
    return;
}

/**
 * Convert Hoover structures into an AMQP message and send it
 */
void hoover_send_message( struct hoover_tube *tube,
                          struct hoover_data_obj *hdo,
                          struct hoover_header *header ) {
    amqp_rpc_reply_t reply;
    amqp_basic_properties_t props;
    amqp_table_t *table;
    amqp_bytes_t body;

    /* convert HDO to amqp_bytes_t */
    body.len = hdo->size;
    body.bytes = hdo->data;

    /* create the amqp_table that contains the header metadata */
    table = create_amqp_header_table( header );

    /* TODO: figure out what these flags mean */
    memset( &props, 0, sizeof(props) );
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_HEADERS_FLAG;
    props.delivery_mode = 2; /* 1 or 2? */
    props.headers = *table;

    /* Send the actual AMQP message */
    amqp_basic_publish(
        tube->connection,   /* amqp_connection_state_t state */
        tube->channel,      /* amqp_channel_t channel */
        tube->exchange,     /* amqp_bytes_t exchange */
        tube->routing_key,  /* amqp_bytes_t routing_key */
        0,                  /* amqp_boolean_t mandatory */
        0,                  /* amqp_boolean_t immediate */
        &props,             /* amqp_basic_properties_t *properties */
        body                /* amqp_bytes_t body */
    );

    free_amqp_header_table(table);

    reply = amqp_get_rpc_reply(tube->connection);
    parse_amqp_response(reply, "publish message", true);

    return;
}
