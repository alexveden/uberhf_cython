# IMPORTANT: dont' forget to include sources in the module .pyx file
#  # distutils: sources = uberhf/includes/hashmap.c uberhf/includes/safestr.c

from libc.stdint cimport uint64_t, uint16_t

cdef extern from "hashmap.h"  nogil:
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