# IMPORTANT: dont' forget to include sources in the module .pyx file
#  # distutils: sources = uberhf/includes/hashmap.c uberhf/includes/safestr.c

from libc.stdint cimport uint64_t, uint16_t

cdef extern from "hashmapsrc.h"  nogil:
    ctypedef uint64_t item_hash_func(const void *item, uint64_t seed0, uint64_t seed1)
    ctypedef int item_compare_func(const void *a, const void *b, void *udata)

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

    hashmap * hashmap_new(size_t elsize,
                          size_t cap,
                          uint64_t seed0,
                          uint64_t seed1,
                          uint64_t (*hash)(const void *item,
                                           uint64_t seed0, uint64_t seed1),
                          int (*compare)(const void *a, const void *b,
                                         void *udata),
                          void (*elfree)(void *item),
                          void *udata)
    void hashmap_free(hashmap * map)

    void * hashmap_set(hashmap * map, const  void *item)
    void * hashmap_get(hashmap * map, const  void *item)
    void hashmap_clear(hashmap *map, bint update_cap)
    void * hashmap_delete(hashmap *map, void *item)
    size_t hashmap_count(hashmap * map)
    bint hashmap_iter(hashmap *map, size_t *i, void **item)


cdef class HashMap:
    cdef hashmap* _hash_map

    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil

    # THIS must be called in child HashMap class, because just overriding is not enough
    # def __cinit__(self):
    #     self._new(sizeof(ItemOrStruct), self.item_hash, self.item_compare, 16)

    @staticmethod
    cdef uint64_t hash_func(const void *data, size_t data_len, uint64_t seed0, uint64_t seed1) nogil

    cdef void _new(self,
                   size_t item_size,
                   uint64_t (*item_hash_f)(const void *item, uint64_t seed0, uint64_t seed1) nogil,
                   int (*item_compare_f)(const void *a, const void *b, void *udata) nogil,
                   size_t capacity) nogil

    cdef void* set(self, void *item)  nogil
    cdef void* get(self, void *item)  nogil
    cdef size_t count(self)  nogil
    cdef void clear(self)  nogil
    cdef void* delete(self, void *item)  nogil
    cdef bint iter(self, size_t *i, void **item)  nogil

