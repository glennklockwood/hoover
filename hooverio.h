#include <limits.h>

#ifdef __APPLE__
    #include <sys/syslimits.h>
    #define MAX_PATH PATH_MAX
    #define HOST_NAME_MAX _POSIX_HOST_NAME_MAX
#endif

#include <openssl/sha.h>

#ifndef HOOVER_BLK_SIZE
    #define HOOVER_BLK_SIZE 128 * 1024
#endif

#ifndef HOOVER_JOB_ID_VAR
    #define HOOVER_JOB_ID_VAR "SLURM_JOB_ID"
#endif
#ifndef HOOVER_TASK_ID_VAR
    #define HOOVER_TASK_ID_VAR "SLURM_STEP_ID"
#endif

#define SHA_DIGEST_LENGTH_HEX (SHA_DIGEST_LENGTH * 2 + 1)
#define COMPRESS_FIELD_LEN 8
#define TASK_ID_LEN 64

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
    char compression[COMPRESS_FIELD_LEN];
};

/* when adding new header entries, you must also modify create_amqp_header_table
 * and build_hoover_header
 */
struct hoover_header {
    char filename[MAX_PATH];
    char node_id[HOST_NAME_MAX];
    char task_id[TASK_ID_LEN];
    char compression[COMPRESS_FIELD_LEN];
    unsigned char sha_hash[SHA_DIGEST_LENGTH_HEX];
    size_t size;
};

/*
 * function prototypes
 */
struct hoover_data_obj *hoover_create_hdo( FILE *fp, size_t block_size );
size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size );
void free_hdo( struct hoover_data_obj *hdo );

struct hoover_header *build_hoover_header( char *filename, struct hoover_data_obj *hdo );
void free_hoover_header( struct hoover_header *header );
char *serialize_header(struct hoover_header *header);

char *build_manifest( struct hoover_header **hoover_headers, int num_headers );
struct hoover_data_obj *manifest_to_hdo( char *manifest, size_t manifest_size );

int get_hoover_node_id( char *name, size_t len );
int get_hoover_task_id( char *name, size_t len );
