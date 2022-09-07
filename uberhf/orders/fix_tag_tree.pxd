from libc.stdint cimport uint16_t

ctypedef struct FIXOffsetMap:
    uint16_t tag
    uint16_t data_offset

ctypedef struct FIXTagBinaryTree:
    uint16_t magic
    uint16_t size
    uint16_t capacity
    uint16_t last_tag
    uint16_t last_tag_idx
    FIXOffsetMap * elements


cdef FIXTagBinaryTree * binary_tree_create(uint16_t initial_capacity) nogil
cdef FIXTagBinaryTree * binary_tree_shadow(void * data, size_t data_size, void * elements, size_t el_size) nogil
cdef void binary_tree_destroy(FIXTagBinaryTree * tree) nogil
cdef bint binary_tree_resize(FIXTagBinaryTree * tree, size_t new_capacity) nogil
cdef uint16_t binary_tree_set_offset(FIXTagBinaryTree * tree, uint16_t tag, uint16_t tag_offset) nogil
cdef uint16_t binary_tree_get_offset(FIXTagBinaryTree * tree, uint16_t tag) nogil
