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
    int8_t last_error

    # Tag tree values
    uint8_t tags_count
    uint8_t tags_capacity
    uint16_t tags_last
    uint8_t tags_last_idx

    # Quick tag offsets
    uint16_t clord_tag_offset


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
            data_size +                                 # self.values
            sizeof(uint16_t) +                          # Magic middle
            sizeof(FIXOffsetMap) * tag_tree_capacity +  # self.tags
            sizeof(uint16_t)                            # Magic end
            )


cdef inline void* _calc_offset_values(FIXMsgStruct * self) nogil:
    return (<void *> self + sizeof(FIXMsgStruct))                 # Header


cdef inline uint16_t * _calc_offset_magic_middle(FIXMsgStruct * self) nogil:
    return <uint16_t *>(<void*>self + sizeof(FIXMsgStruct) +              # Header
                        self.header.data_size                             # self.values
                        )


cdef inline FIXOffsetMap* _calc_offset_tags(FIXMsgStruct * self) nogil:
    return <FIXOffsetMap*> (<void *> self + sizeof(FIXMsgStruct) +              # Header
                            self.header.data_size   +                           # self.values
                            sizeof(uint16_t)                                    # Magic middle
                            )


cdef inline uint16_t * _calc_offset_magic_end(FIXMsgStruct * self) nogil:
    return <uint16_t *> (<void *> self +
                         sizeof(FIXMsgStruct) +                              # Header
                         self.header.data_size  +                            # self.values
                         sizeof(uint16_t) +                                  # Magic middle
                         sizeof(FIXOffsetMap) * self.header.tags_capacity    # self.tags
                         )

cdef class FIXMsg:
    @staticmethod
    cdef FIXMsgStruct * create(char msg_type, uint16_t data_size, uint8_t tag_tree_capacity) nogil

    @staticmethod
    cdef void destroy(FIXMsgStruct * self) nogil

    @staticmethod
    cdef size_t size(FIXMsgStruct * self) nogil

    @staticmethod
    cdef FIXMsgStruct * copy(FIXMsgStruct * self) nogil

    @staticmethod
    cdef FIXMsgStruct * check_buffer(void * buffer, size_t buf_len) nogil

    @staticmethod
    cdef int get_last_error(FIXMsgStruct * self) nogil

    @staticmethod
    cdef const char * get_last_error_str(int e) nogil

    @staticmethod
    cdef bint is_valid(FIXMsgStruct * self) nogil

    @staticmethod
    cdef int has_capacity(FIXMsgStruct * self, uint8_t add_tags, uint16_t new_rec_size) nogil

    @staticmethod
    cdef int set(FIXMsgStruct * self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil

    @staticmethod
    cdef int get(FIXMsgStruct * self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil

    @staticmethod
    cdef int replace(FIXMsgStruct * self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil

    @staticmethod
    cdef uint16_t _set_tag_offset(FIXMsgStruct * self, uint16_t tag, uint16_t tag_offset) nogil

    @staticmethod
    cdef uint16_t _get_tag_offset(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef FIXMsgStruct * resize(FIXMsgStruct * self, uint8_t add_tags, uint16_t add_values_size) nogil

    @staticmethod
    cdef int group_start(FIXMsgStruct * self, uint16_t group_tag, uint16_t grp_n_elements, uint16_t n_tags, uint16_t *tags) nogil
    @staticmethod
    cdef int group_add_tag(FIXMsgStruct * self, uint16_t group_tag, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil
    @staticmethod
    cdef int group_finish(FIXMsgStruct * self, uint16_t group_tag) nogil
    @staticmethod
    cdef int group_get(FIXMsgStruct * self, uint16_t group_tag, uint16_t el_idx, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil
    @staticmethod
    cdef int group_count(FIXMsgStruct * self, uint16_t group_tag) nogil

    #
    # Generic type get/set
    #
    @staticmethod
    cdef int set_int(FIXMsgStruct * self, uint16_t tag, int value) nogil
    @staticmethod
    cdef int * get_int(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_bool(FIXMsgStruct * self, uint16_t tag, bint value) nogil
    @staticmethod
    cdef int8_t * get_bool(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_char(FIXMsgStruct * self, uint16_t tag, char value) nogil
    @staticmethod
    cdef char * get_char(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_double(FIXMsgStruct * self, uint16_t tag, double value) nogil
    @staticmethod
    cdef double * get_double(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_utc_timestamp(FIXMsgStruct * self, uint16_t tag, long value_ns) nogil
    @staticmethod
    cdef long * get_utc_timestamp(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_str(FIXMsgStruct * self, uint16_t tag, char *value, uint16_t length) nogil
    @staticmethod
    cdef char * get_str(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_uint64(FIXMsgStruct * self, uint16_t tag, uint64_t value) nogil
    @staticmethod
    cdef uint64_t * get_uint64(FIXMsgStruct * self, uint16_t tag) nogil

    @staticmethod
    cdef int set_long(FIXMsgStruct * self, uint16_t tag, long value) nogil
    @staticmethod
    cdef long * get_long(FIXMsgStruct * self, uint16_t tag) nogil