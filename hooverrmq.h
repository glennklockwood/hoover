#include <amqp_ssl_socket.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#ifndef HOOVER_MAX_SERVERS
#define HOOVER_MAX_SERVERS 256
#endif

#ifndef HOOVER_CONFIG_FILE
#define HOOVER_CONFIG_FILE "/etc/opt/nersc/slurmd_log_rotate_mq.conf"
#endif

/*
 * Global structures
 */
struct hoover_tube_config {
    char *servers[HOOVER_MAX_SERVERS];
    int max_hosts;
    int remaining_hosts;
    int port;
    char *vhost;
    char *username;
    char *password;
    char *exchange;
    char *exchange_type;
    char *queue;
    char *routing_key;
    size_t max_transmit_size;
    int use_ssl;
};

/* Each hoover_tube just aggregates a connection, a socket, a channel, and an
   exchange into a single object for simplicity.  For simple message passing,
   we only need to define one of each to send messages. */
struct hoover_tube {
    amqp_socket_t *socket;
    amqp_channel_t channel; /* I have no idea why channel passed around by value by rabbitmq-c */
    amqp_connection_state_t connection;
    amqp_bytes_t exchange;
    amqp_bytes_t routing_key;
};

struct hoover_tube *create_hoover_tube(struct hoover_tube_config *config);
void free_hoover_tube(struct hoover_tube *tube);

struct hoover_tube_config *read_tube_config();
void save_tube_config(struct hoover_tube_config *config, FILE *out);
void free_tube_config(struct hoover_tube_config *config);

void hoover_send_message(struct hoover_tube *tube,
                         struct hoover_data_obj *hdo,
                         struct hoover_header *header);
