from libc.stdint cimport uint64_t, uint16_t

DEF TICKER_LEN = 30

cdef extern from "../include/hashmap.h":
    cdef extern struct hashmap:
        void *(*malloc)(size_t);
        void *(*realloc)(void *, size_t);
        void (*free)(void *);
        bint oom;
        size_t elsize;
        size_t cap;
        uint64_t seed0;
        uint64_t seed1;
        uint64_t (*hash)(const void *item, uint64_t seed0, uint64_t seed1);
        int (*compare)(const void *a, const void *b, void *udata);
        void (*elfree)(void *item);
        void *udata;
        size_t bucketsz;
        size_t nbuckets;
        size_t count;
        size_t mask;
        size_t growat;
        size_t shrinkat;
        void *buckets;
        void *spare;
        void *edata;

    uint64_t hashmap_sip(const void *data, size_t len, uint64_t seed0, uint64_t seed1)
    void hashmap_free(hashmap * map)
    hashmap * hashmap_new(size_t elsize,
                          size_t cap,
                          uint64_t seed0,
                          uint64_t seed1,
                          uint64_t (*hash)(const void *item, uint64_t seed0, uint64_t seed1),
                          int (*compare)(const void *a, const void *b, void *udata),
                          void (*elfree)(void *item),
                          void *udata)
    void *hashmap_set(hashmap * map, const  void *item);
    void *hashmap_get(hashmap * map, const  void *item);

ctypedef struct QPoolHeader:
    int magic_number
    int count
    int capacity
    int last_quote_utc
    int last_upd_utc
    int n_errors

# ticker_idx and QRec - must have char ticker[TICKER_LEN] at the first place
#    to allow us looking hashmap transparently
#
ctypedef struct TickerIdx:
    char ticker[TICKER_LEN]
    int idx_position

ctypedef struct QRec:
    char ticker[TICKER_LEN]
    uint16_t crc_b
    double bid
    double ask
    double last
    double bid_size
    double ask_size
    long last_upd_utc
    uint16_t crc_e


cdef class MemPoolQuotes:
    cdef readonly int pool_capacity
    cdef readonly int pool_cnt
    cdef readonly int n_errors
    cdef readonly long magic_number
    cdef readonly object shared_mem_file

    cdef QPoolHeader * header
    cdef QRec * quotes
    cdef void * pool_buffer
    cdef hashmap * pool_map


    @staticmethod
    cdef int ticker_compare(const void *a, const void *b, void *udata)

    @staticmethod
    cdef uint64_t ticker_hash(const void *item, uint64_t seed0, uint64_t seed1)

    cdef int quote_update(self, QRec * q)
    cdef bint quote_reset(self, char *ticker, QRec *q)
    cdef QRec* quote_get(self, char *ticker)

