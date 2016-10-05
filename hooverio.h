#ifdef __APPLE__
#include <sys/syslimits.h>
#define MAX_PATH PATH_MAX
#endif

#include <openssl/sha.h>

#ifndef HOOVER_BLK_SIZE
#define HOOVER_BLK_SIZE 128 * 1024
#endif

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

/*
 * hoover_data_obj describes a file that has been loaded into memory through
 *   hoover_create_hdo().  If this were C++, it would be derived from
 *   amqp_bytes_t
 */
struct hoover_data_obj {
    void *data;
    size_t size;
    char hash[SHA_DIGEST_LENGTH_HEX];
    char hash_orig[SHA_DIGEST_LENGTH_HEX];
    size_t size_orig;
};

struct hoover_header {
    char filename[MAX_PATH];
    size_t size;
    unsigned char hash[SHA_DIGEST_LENGTH_HEX];
};

/*
 * function prototypes
 */

struct hoover_data_obj *hoover_create_hdo( FILE *fp, size_t block_size );
size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size );
void free_hdo( struct hoover_data_obj *hdo );

struct hoover_header *build_hoover_header( char *filename, struct hoover_data_obj *hdo );
void free_hoover_header( struct hoover_header *header );
