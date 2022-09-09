"""
Universal binary message container for FIX protocol
"""
from libc.stdint cimport uint64_t, uint16_t, int8_t, uint8_t
from uberhf.orders.fix_tag_tree cimport FIXTagBinaryTree

ctypedef struct FIXHeader:
    uint16_t magic_number
    char msg_type
    uint16_t last_position
    uint16_t data_size
    uint8_t n_reallocs
    uint8_t tag_errors
    uint8_t is_read_only

    # Tag tree values
    uint8_t tags_count
    uint8_t tags_capacity
    uint16_t tags_last
    uint8_t tags_last_idx


ctypedef struct FIXRec:
    uint16_t tag
    char value_type
    uint16_t value_len

ctypedef struct FIXGroupRec:
    FIXRec fix_rec
    uint16_t grp_n_elements
    uint16_t n_tags
    uint16_t current_element
    int8_t current_tag_len

ctypedef struct FIXOffsetMap:
    uint16_t tag
    uint16_t data_offset


ctypedef struct FIXMsgStruct:
    FIXHeader header
    FIXGroupRec * open_group
    FIXOffsetMap * tags
    void * values






cdef class FIXMsg:
    @staticmethod
    cdef FIXMsgStruct * create(char msg_type, uint16_t data_size, uint16_t tag_tree_capacity) nogil

    @staticmethod
    cdef void destroy(FIXMsgStruct * self) nogil

    @staticmethod
    cdef bint is_valid(FIXMsgStruct * self) nogil

    @staticmethod
    cdef int set(FIXMsgStruct * self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil

    @staticmethod
    cdef int get(FIXMsgStruct * self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil

    @staticmethod
    cdef uint16_t _set_tag_offset(FIXMsgStruct * self, uint16_t tag, uint16_t tag_offset) nogil

    @staticmethod
    cdef uint16_t _get_tag_offset(FIXMsgStruct * self, uint16_t tag) nogil