"""
Universal binary message container for FIX protocol
"""
from libc.stdint cimport uint64_t, uint16_t, int8_t
from uberhf.orders.fix_tag_tree cimport FIXTagBinaryTree


ctypedef struct FIXBinaryHeader:
    uint16_t magic_number
    char msg_type
    uint16_t last_position
    uint16_t data_size
    uint16_t n_reallocs
    uint16_t tag_errors


ctypedef struct FIXRec:
    uint16_t tag
    char value_type
    uint16_t value_len



ctypedef struct GroupRec:
    FIXRec fix_rec
    uint16_t grp_n_elements
    uint16_t n_tags
    uint16_t current_element
    int8_t current_tag_len


cdef class FIXBinaryMsg:
    cdef void* _data

    cdef void* values
    cdef GroupRec* open_group
    cdef FIXTagBinaryTree * tag_tree
    cdef FIXBinaryHeader* header

    cdef int _request_new_space(self, size_t extra_size) nogil
    cdef bint is_valid(self) nogil
    cdef int set(self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil
    cdef int get(self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil

    cdef int group_start(self, uint16_t group_tag, uint16_t grp_n_elements, uint16_t n_tags, uint16_t *tags) nogil
    cdef int group_add_tag(self, uint16_t group_tag, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil
    cdef int group_finish(self, uint16_t group_tag) nogil
    cdef int group_get(self, uint16_t group_tag, uint16_t el_idx, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil
    cdef int group_count(self, uint16_t group_tag) nogil

