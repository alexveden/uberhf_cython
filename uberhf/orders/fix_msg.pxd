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



cdef inline size_t _calc_data_size(uint16_t data_size, uint8_t tag_tree_capacity) nogil:
    return (sizeof(FIXMsgStruct) +                      # Header
            sizeof(FIXOffsetMap) * tag_tree_capacity +  # Tag index
            sizeof(uint16_t) +                          # Magic middle
            data_size +                                 # Data
            sizeof(uint16_t)                            # Magic end
            )

cdef inline FIXOffsetMap* _calc_offset_tags(FIXMsgStruct * self) nogil:
    return <FIXOffsetMap*> (<void *> self + sizeof(FIXMsgStruct))

cdef inline uint16_t * _calc_offset_magic_middle(FIXMsgStruct * self) nogil:
    return <uint16_t *>(<void*>self + sizeof(FIXMsgStruct) +              # Header
                        sizeof(FIXOffsetMap) * self.header.tags_capacity  # Tag index
                        )

cdef inline void* _calc_offset_values(FIXMsgStruct * self) nogil:
    return (<void *> self + sizeof(FIXMsgStruct) +                # Header
            sizeof(FIXOffsetMap) * self.header.tags_capacity +    # Tag index
            sizeof(uint16_t)                                      # Magic middle
            )

cdef inline uint16_t * _calc_offset_magic_end(FIXMsgStruct * self) nogil:
    return <uint16_t *> (<void *> self +
                         sizeof(FIXMsgStruct) +                              # Header
                         sizeof(FIXOffsetMap) * self.header.tags_capacity +  # tag index
                         sizeof(uint16_t) +                                  # Magic middle
                         self.header.data_size                               # Data
                         )





cdef class FIXMsg:
    @staticmethod
    cdef FIXMsgStruct * create(char msg_type, uint16_t data_size, uint8_t tag_tree_capacity) nogil

    @staticmethod
    cdef void destroy(FIXMsgStruct * self) nogil

    @staticmethod
    cdef bint is_valid(FIXMsgStruct * self) nogil

    @staticmethod
    cdef bint has_capacity(FIXMsgStruct * self, uint8_t add_tags, uint16_t new_rec_size) nogil

    @staticmethod
    cdef int set(FIXMsgStruct * self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil

    @staticmethod
    cdef int get(FIXMsgStruct * self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil

    @staticmethod
    cdef uint16_t _set_tag_offset(FIXMsgStruct * self, uint16_t tag, uint16_t tag_offset) nogil

    @staticmethod
    cdef uint16_t _get_tag_offset(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef FIXMsgStruct * resize(FIXMsgStruct * self, uint8_t add_tags, uint16_t add_values_size) nogil