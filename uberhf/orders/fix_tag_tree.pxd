from uberhf.orders.fix_binary_msg cimport FIXOffsetMap
from libc.stdint cimport uint16_t


ctypedef struct FIXTagBinaryTree:
    uint16_t magic
    uint16_t size
    uint16_t capacity
    FIXOffsetMap * elements


cdef FIXTagBinaryTree * binary_tree_create(uint16_t initial_capacity) nogil
cdef FIXTagBinaryTree * binary_tree_shadow(void * data, size_t data_size, void * elements, size_t el_size) nogil
cdef void binary_tree_destroy(FIXTagBinaryTree * tree) nogil
cdef bint binary_tree_resize(FIXTagBinaryTree * tree, size_t new_capacity) nogil
cdef uint16_t binary_tree_set_offset(FIXTagBinaryTree * tree, uint16_t tag, uint16_t tag_offset) nogil
cdef uint16_t binary_tree_get_offset(FIXTagBinaryTree * tree, uint16_t tag) nogil
