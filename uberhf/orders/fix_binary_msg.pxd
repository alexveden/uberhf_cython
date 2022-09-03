"""
Universal binary message container for FIX protocol
"""
from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMapBase


ctypedef struct FIXBinaryHeader:
    uint16_t magic_number
    char msg_type
    uint16_t last_position
    uint16_t data_size
    uint16_t n_reallocs
    uint16_t tag_duplicates


ctypedef struct FIXRec:
    uint16_t tag
    char value_type
    uint16_t value_len


ctypedef struct FIXOffsetMap:
    uint16_t tag
    uint16_t data_offset


cdef class FIXTagHashMap(HashMapBase):
    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil
    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil


cdef class FIXBinaryMsg:
    cdef void* _data

    cdef void* values
    cdef void* groups
    cdef FIXTagHashMap tag_hashmap
    cdef FIXBinaryHeader* header

    cdef int _resize_data(self, uint16_t new_size) nogil
    cdef int set(self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil
    cdef int get(self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil

