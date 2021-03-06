/*******************************************************************************
 *  hooverio.c
 *
 *  Components of Hoover that are responsible for file I/O and processing data
 *  streams (compression, checksumming, etc)
 *
 *  Glenn K. Lockwood, Lawrence Berkeley National Laboratory       October 2016
 ******************************************************************************/

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600 /* for gethostname in unistd.h */
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h> /* for debugging */
#include <zlib.h>

#include "hooverio.h"

/*******************************************************************************
 *  local prototypes and structs
 ******************************************************************************/
struct block_state_structs *init_block_states( void );
int *finalize_block_states( struct block_state_structs *bss );

/*
 * block_state_structs is just a container for the state structs that belong
 *   to all of the block processing algorithms (compression, encryption, etc)
 *   that may be used by hooverio
 */
struct block_state_structs {
    SHA_CTX sha_stream;
    SHA_CTX sha_stream_compressed;
    z_stream z_stream; 
    char compression[COMPRESS_FIELD_LEN];
    unsigned char sha_hash[SHA_DIGEST_LENGTH];
    unsigned char sha_hash_compressed[SHA_DIGEST_LENGTH];
    char sha_hash_hex[SHA_DIGEST_LENGTH_HEX];
    char sha_hash_compressed_hex[SHA_DIGEST_LENGTH_HEX];
};


/*******************************************************************************
 * internal functions
 ******************************************************************************/
struct block_state_structs *init_block_states( void ) {
    struct block_state_structs *bss;

    bss = malloc(sizeof(*bss));
    if ( !bss ) return NULL;

    /* init SHA calculator */
    SHA1_Init( &(bss->sha_stream) );
    SHA1_Init( &(bss->sha_stream_compressed) );
    memset( bss->sha_hash, 0, SHA_DIGEST_LENGTH );
    memset( bss->sha_hash_compressed, 0, SHA_DIGEST_LENGTH );
    memset( bss->sha_hash_hex, 0, SHA_DIGEST_LENGTH_HEX );
    memset( bss->sha_hash_compressed_hex, 0, SHA_DIGEST_LENGTH_HEX );

    /* init zlib calculator */
    (bss->z_stream).zalloc = Z_NULL;
    (bss->z_stream).zfree = Z_NULL;
    (bss->z_stream).opaque = Z_NULL;

    if ( (deflateInit2(
            &(bss->z_stream),
            Z_DEFAULT_COMPRESSION,
            Z_DEFLATED,
            15 + 16, /* 15 is default for deflateInit, +16 enables gzip */
            8,
            Z_DEFAULT_STRATEGY)) != Z_OK ) {
        free(bss);
        return NULL;
    }
    strncpy(bss->compression, "gz", COMPRESS_FIELD_LEN);

    return bss;
}

int *finalize_block_states( struct block_state_structs *bss ) {
    int i;

    deflateEnd( &(bss->z_stream) );
    SHA1_Final(bss->sha_hash, &(bss->sha_stream));
    SHA1_Final(bss->sha_hash_compressed, &(bss->sha_stream_compressed));

    for ( i = 0; i < SHA_DIGEST_LENGTH; i++ )
    {
        sprintf( (char*)&(bss->sha_hash_hex[2*i]), "%02x", bss->sha_hash[i] );
        sprintf( (char*)&(bss->sha_hash_compressed_hex[2*i]), "%02x", bss->sha_hash_compressed[i] );
    }
    /* final character is \0 from when bss was initialized */

    return 0;
}

/*
 * Read a file block by block, and pass these blocks through block-based
 * algorithms (hashing, compression, etc)
 */
struct hoover_data_obj *hoover_create_hdo( FILE *fp, size_t block_size ) {
    void *buf,
         *out_buf,
         *p_out;
    size_t out_buf_len,
           bytes_read,
           bytes_written,
           tot_bytes_read = 0,
           tot_bytes_written = 0;
    struct block_state_structs *bss;
    struct hoover_data_obj *hdo;
    struct stat st;
    int fail = 0;
    int flush;

    /* buf is filled from file, then processed (compress+hash) */
    if ( !(buf = malloc( block_size )) ) return NULL;

    /* get file size so we know how big to allocate our buffer */
    if ( fstat(fileno(fp), &st) != 0 ) {
        free( buf );
        return NULL;
    }

    /* worst-case, compression adds +10%; ideally it will reduce size */
    out_buf_len = st.st_size * 1.1;
    out_buf = malloc(out_buf_len);
    if ( !out_buf ) {
        free(buf);
        return NULL;
    }
    p_out = out_buf;

    /* initialize block-based algorithm state stuctures here */
    if ( !(bss = init_block_states()) ) {
        free(buf);
        free(out_buf);
        return NULL;
    }

    do { /* loop until no more input */
        bytes_read = fread(buf, 1, block_size, fp);

        if ( feof(fp) )
            flush = Z_FINISH;
        else
            flush = Z_NO_FLUSH;

        tot_bytes_read += bytes_read;
        /* update the SHA1 of the pre-compressed data */
        SHA1_Update( &(bss->sha_stream), buf, bytes_read );

        /* set start of compression block */
        (bss->z_stream).avail_in = bytes_read;
        (bss->z_stream).next_in = (unsigned char*)buf;

        do { /* loop until no more output */
            /* avail_out = how big is the output buffer */
            (bss->z_stream).avail_out = out_buf_len - tot_bytes_written;
            /* next_out = pointer to the output buffer */
            (bss->z_stream).next_out = (unsigned char*)out_buf + tot_bytes_written;

            /* deflate updates avail_in and next_in as it consumes input data.
               it may also update avail_out and next_out if it flushed any data,
               but this is not necessarily the case since zlib may internally
               buffer data */
            if ( (deflate(&(bss->z_stream), flush)) != Z_OK ) {
                fail = 1;
                break;
            }
            bytes_written = ( (char*)((bss->z_stream).next_out) - (char *)p_out );
            tot_bytes_written += bytes_written;

            /* update the SHA1 of the compressed */
            SHA1_Update( &(bss->sha_stream_compressed), p_out, bytes_written );

            /* update the pointer - there may be a cleaner way to do this */
            p_out = (bss->z_stream).next_out;
        } while ( (bss->z_stream).avail_out == 0 );
        if ( fail ) break;
    } while ( bytes_read != 0 ); /* loop until we run out of input */

    assert( bss->z_stream.avail_in == 0 );
    if ( bss->z_stream.avail_out != 0 )
    {
        deflate(&(bss->z_stream), flush);
        bytes_written = ( (char*)((bss->z_stream).next_out) - (char *)p_out );
        tot_bytes_written += bytes_written;
        SHA1_Update( &(bss->sha_stream_compressed), p_out, bytes_written );
    }

    assert( flush == Z_FINISH );

    /* finalize block-based algorithm state structures here */
    finalize_block_states( bss );

    /* create the hoover data object */
    hdo = malloc(sizeof(*hdo));
    if (!hdo) {
        free(buf);
        free(out_buf);
        return NULL;
    }
    strncpy( hdo->hash, bss->sha_hash_compressed_hex, SHA_DIGEST_LENGTH_HEX );
    strncpy( hdo->hash_orig, bss->sha_hash_hex, SHA_DIGEST_LENGTH_HEX );
    
    hdo->size = tot_bytes_written;
    hdo->data = realloc( out_buf, tot_bytes_written );
    hdo->size_orig = tot_bytes_read;
    strncpy(hdo->compression, bss->compression, COMPRESS_FIELD_LEN);

    /* clean up */
    free(buf);

    return hdo;
}

/* free memory associated with a hoover data object */
void free_hdo( struct hoover_data_obj *hdo ) {
    if ( hdo == NULL ) {
        fprintf( stderr, "free_hdo: received NULL pointer\n" );
    }
    else {
        free( hdo->data );
        free( hdo );
    }
    return;
}

void free_hoover_header( struct hoover_header *header ) {
    if ( header == NULL )
        fprintf( stderr, "free_hoover_header: received NULL pointer\n" );
    else
        free(header);
    return;
}

/*
 * Generate the contents of a manifest file based on generated headers
 *
 * input: list of hoover headers
 * output: serialized list of hoover_headers (i.e., a json blob)
 *
 * Caveats:
 *   1. This is poor-man's JSON encoding.  I am not proud of it.
 *   2. We use very naive and inefficient memory management here--if
 *      performance is ever critical, this needs to be rewritten.
 */
char *build_manifest( struct hoover_header **hoover_headers, int num_headers ) {
    int i;
    char *buf;
    char *manifest = NULL;
    const char *manifest_head = "[",
               *manifest_tail = "]",
               *manifest_join = ",";
    /* nomenclature:  _len = product of strlen (no terminal \0 included)
                     _size = number of bytes (must include terminal \0) */
    size_t manifest_join_len = strlen(manifest_join);
    size_t manifest_size, record_len;


    /* initialize manifest */
    manifest_size = sizeof(*manifest) * (strlen(manifest_head) + strlen(manifest_tail) + 1);
    manifest = malloc(manifest_size);
    strcpy( manifest, manifest_head );

    /* add each header */
    for (i = 0; i < num_headers; i++) {
        buf = serialize_header(hoover_headers[i]);
        /* the manifest_join_len is not necessary for i = 0, but whatever */
        manifest_size += strlen(buf) + manifest_join_len;
        manifest = realloc(manifest, manifest_size);

        if (i > 0) {
            strcat(manifest, manifest_join);
        }
        strcat(manifest, buf);
        free(buf);
    }
    strcat( manifest, manifest_tail );
    return manifest;
}

/*
 * Generate the hoover_header struct from a file
 */
struct hoover_header *build_hoover_header( char *filename, struct hoover_data_obj *hdo, char *filetype ) {
    struct hoover_header *header;
    struct stat st;
    char *serialized;

    header = malloc(sizeof(*header));
    if ( !header )
        return NULL;
    memset(header,0,sizeof(*header));

    /*
     * header->filename
     * header->node_id
     * header->task_id
     * header->compress
     * header->sha_hash
     * header->type
     * header->size
     */
    strncpy(header->filename, filename, PATH_MAX);
    get_hoover_node_id(header->node_id, HOST_NAME_MAX);
    get_hoover_task_id(header->task_id, TASK_ID_LEN);
    strncpy(header->compression, hdo->compression, COMPRESS_FIELD_LEN);
    strncpy((char*)header->sha_hash, (const char*)hdo->hash, SHA_DIGEST_LENGTH_HEX);
    header->size = hdo->size;
    strncpy(header->type, filetype, HDO_TYPE_FIELD_LEN);

    /* if compressed, append the compression suffix to the transmitted file
       name.  this keeps the consumer from having to explicitly know anything
       about the HDO payload */
    if ( header->compression[0] != '\0' ) {
        strncat(header->filename, ".", PATH_MAX);
        strncat(header->filename, header->compression, PATH_MAX);
    }

    return header;
}

/*
 *  Get a unique node identifier for this host; used in Hoover headers
 */
int get_hoover_node_id( char *name, size_t len ) {
    return gethostname( name, len );
}

/*
 *  Get a unique task identifier for this invocation of Hoover
 */
int get_hoover_task_id( char *name, size_t len ) {
    char *jobid, *taskid;
    jobid = getenv(HOOVER_JOB_ID_VAR);
    taskid = getenv(HOOVER_TASK_ID_VAR);
    int written;

    if (!jobid && !taskid) {
        written = snprintf(name, len, "%d", getpid());
    }
    else if (!jobid) {
        written = snprintf(name, len, "0-%s", taskid);
    }
    else if (!taskid) {
        written = snprintf(name, len, "%s-0", jobid);
    }
    else {
        written = snprintf( name, len, "%s-%s", jobid, taskid );
    }

    return !(len == written);
}

/*
 * Serialized representation of a header.  Currently implemented as poor-man's
 * JSON encoder.
 */
char *serialize_header(struct hoover_header *header) {
    size_t len;
    char *buf;

    const char *template = "{ \"filename\": \"%s\", \"node_id\": \"%s\", \"task_id\": \"%s\", \"compression\": \"%s\", \"sha1sum\": \"%s\", \"size\": %ld, \"type\": \"%s\" }";

    /* assume header is mostly fixed-size characters */
    /* +24 chars = string representation up to a yottabyte */
    len = sizeof(*header)+24;

    if (!(buf = malloc(len)))
        return NULL;

    snprintf( buf, len, template,
        header->filename,
        header->node_id,
        header->task_id,
        header->compression,
        header->sha_hash,
        header->size,
        header->type );
/*  printf( "serialize_header: trimming from %ld to %ld (strlen=%ld)\n",
        sizeof(*header)+24,
        sizeof(*buf) * strlen(buf) + 1,
        strlen(buf) );
        */
    buf = realloc(buf, sizeof(*buf) * strlen(buf) + 1);

    return buf;
}

/*
 *  Convert a serialized manifest to an HDO so it can be sent over the wire.
 *
 *  Note that manifest_size should not include the terminal NULL character, as
 *  we do not want that that appearing in the HDO payload.
 */
struct hoover_data_obj *manifest_to_hdo( char *manifest, size_t manifest_size ) {
    /* Copy the manifest into a temporary file since hoover_create_hdo operates
     * on FILE pointers.  On POSIX2008 systems we can use fmemopen(3) or
     * open_memstream(3), but these aren't available on all systems (notably,
     * macOS)
     */
    FILE *fp = tmpfile();
    size_t bytes_left = manifest_size;
    char *p = manifest;
    do {
        size_t write_size;
        if ( bytes_left > HOOVER_BLK_SIZE )
            write_size = HOOVER_BLK_SIZE;
        else
            write_size = bytes_left;
        size_t bytes_written = fwrite( p, 1, write_size, fp );
        if ( bytes_written != write_size ) {
            perror("manifest_to_hoover: unable to write out manifest");
            return NULL;
        }
        else {
            /* there is redundant state being tracked here, but this is easy */
            p += bytes_written;
            bytes_left -= bytes_written;
        }
    } while ( bytes_left != 0 );

    /* Create the HDO from the file we just populated */
    if (fseek(fp, 0, SEEK_SET) != 0) {
        perror("manifest_to_hoover: unable to rewind manifest file");
        return NULL;
    }
    struct hoover_data_obj *hdo = hoover_create_hdo( fp, HOOVER_BLK_SIZE );

    /* This close automatically unlinks the file due to tmpfile */
    fclose(fp);

    return hdo;
}
