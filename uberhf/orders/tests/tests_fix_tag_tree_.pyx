import time
import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy
from libc.stdint cimport uint64_t, uint16_t
from libc.string cimport memcmp, strlen, strcmp, memcpy, memset
from libc.stdlib cimport malloc, free
from uberhf.prototols.messages cimport *
from uberhf.orders.fix_tag_tree cimport *
from uberhf.orders.fix_binary_msg cimport *
from libc.limits cimport USHRT_MAX


class CyTagTreeTestCase(unittest.TestCase):
    def test_init(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(10)
        assert tree.size == 0
        assert tree.capacity == 10
        assert tree.elements != NULL
        assert tree.magic == 22906
        binary_tree_destroy(tree)


    def test_check_set_offset(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL

        assert binary_tree_set_offset(tree, 2, 20) == 0
        assert tree.elements[0].tag == 2
        assert tree.elements[0].data_offset == 20

        assert binary_tree_set_offset(tree, 3, 30) == 1
        assert tree.elements[0].tag == 2
        assert tree.elements[0].data_offset == 20
        assert tree.elements[1].tag == 3
        assert tree.elements[1].data_offset == 30


        assert binary_tree_set_offset(tree, 1, 10) == 0
        assert tree.elements[0].tag == 1
        assert tree.elements[0].data_offset == 10
        assert tree.elements[1].tag == 2
        assert tree.elements[1].data_offset == 20
        assert tree.elements[2].tag == 3
        assert tree.elements[2].data_offset == 30

        assert binary_tree_set_offset(tree, 5, 50) == 3
        assert tree.elements[0].tag == 1
        assert tree.elements[0].data_offset == 10
        assert tree.elements[1].tag == 2
        assert tree.elements[1].data_offset == 20
        assert tree.elements[2].tag == 3
        assert tree.elements[2].data_offset == 30
        assert tree.elements[3].tag == 5
        assert tree.elements[3].data_offset == 50

        assert binary_tree_set_offset(tree, 4, 40) == 3

        assert tree.elements[0].tag == 1
        assert tree.elements[0].data_offset == 10
        assert tree.elements[1].tag == 2
        assert tree.elements[1].data_offset == 20
        assert tree.elements[2].tag == 3
        assert tree.elements[2].data_offset == 30
        assert tree.elements[3].tag == 4
        assert tree.elements[3].data_offset == 40
        assert tree.elements[4].tag == 5
        assert tree.elements[4].data_offset == 50

        assert binary_tree_get_offset(tree, 1) == 10
        assert binary_tree_get_offset(tree, 2) == 20
        assert binary_tree_get_offset(tree, 3) == 30
        assert binary_tree_get_offset(tree, 4) == 40
        assert binary_tree_get_offset(tree, 5) == 50


        binary_tree_destroy(tree)


    def test_check_set_offset_with_resize(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(1)
        assert tree.size == 0
        assert tree.capacity == 1
        assert tree.elements != NULL

        assert binary_tree_set_offset(tree, 1, 10) == 0
        assert tree.capacity == 1
        assert binary_tree_set_offset(tree, 2, 20) == 1
        assert tree.capacity == 2
        assert binary_tree_set_offset(tree, 3, 30) == 2
        assert tree.capacity == 4
        assert binary_tree_set_offset(tree, 4, 40) == 3
        assert binary_tree_set_offset(tree, 5, 50) == 4
        assert tree.capacity == 8

        assert tree.elements[0].tag == 1
        assert tree.elements[0].data_offset == 10
        assert tree.elements[1].tag == 2
        assert tree.elements[1].data_offset == 20
        assert tree.elements[2].tag == 3
        assert tree.elements[2].data_offset == 30
        assert tree.elements[3].tag == 4
        assert tree.elements[3].data_offset == 40
        assert tree.elements[4].tag == 5
        assert tree.elements[4].data_offset == 50


        assert binary_tree_get_offset(tree, 1) == 10
        assert binary_tree_get_offset(tree, 2) == 20
        assert binary_tree_get_offset(tree, 3) == 30
        assert binary_tree_get_offset(tree, 4) == 40
        assert binary_tree_get_offset(tree, 5) == 50

        binary_tree_destroy(tree)

    def test_check_set_offset_with_errors(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL

        assert binary_tree_set_offset(tree, 1, 10) == 0
        assert binary_tree_set_offset(tree, 1, 20) == USHRT_MAX-2
        assert tree.elements[0].tag == 1
        assert tree.elements[0].data_offset == USHRT_MAX
        assert tree.size == 1
        assert binary_tree_set_offset(tree, 0, 20) == USHRT_MAX
        assert binary_tree_set_offset(tree, USHRT_MAX, 20) == USHRT_MAX

        binary_tree_destroy(tree)

    def test_check_get_offset(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL
        assert binary_tree_get_offset(tree, 1) == USHRT_MAX

        assert binary_tree_set_offset(tree, 1, 10) == 0
        assert binary_tree_set_offset(tree, 2, 20) == 1
        assert binary_tree_set_offset(tree, 3, 30) == 2
        assert binary_tree_set_offset(tree, 4, 40) == 3
        assert binary_tree_set_offset(tree, 5, 50) == 4


        assert binary_tree_get_offset(tree, 1) == 10
        assert binary_tree_get_offset(tree, 2) == 20
        assert binary_tree_get_offset(tree, 3) == 30
        assert binary_tree_get_offset(tree, 4) == 40
        assert binary_tree_get_offset(tree, 5) == 50

        assert binary_tree_get_offset(tree, 0) == USHRT_MAX
        assert binary_tree_get_offset(tree, USHRT_MAX) == USHRT_MAX
        assert binary_tree_get_offset(tree, 6) == USHRT_MAX-1

        binary_tree_destroy(tree)


    def test_overflow(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL

        expected_failure_at = USHRT_MAX - int((USHRT_MAX - sizeof(FIXTagBinaryTree)) / sizeof(FIXOffsetMap))

        for i in range(USHRT_MAX) :
            if i >= USHRT_MAX-10:
                # Overflow
                self.assertEqual(binary_tree_set_offset(tree, i + 1, i), USHRT_MAX,
                                 f'tree.capacity={tree.size}/{tree.capacity}')
            else:
                self.assertEqual(binary_tree_set_offset(tree, i + 1, i), i,
                                 f'tree.capacity={tree.size}/{tree.capacity} expected_failure_at={expected_failure_at}')

        assert tree.capacity == USHRT_MAX-1
        self.assertEqual(tree.size, USHRT_MAX-10)

        binary_tree_destroy(tree)

    def test_resize_and_overflow(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(10000)
        assert binary_tree_resize(tree, 20000) == 1
        assert binary_tree_resize(tree, USHRT_MAX) == 1  # Allow first maximal capacity resize
        assert binary_tree_resize(tree, USHRT_MAX) == 0
        assert binary_tree_resize(tree, 100000000) == 0
        assert tree.capacity == USHRT_MAX-1

        binary_tree_destroy(tree)

    def test_check_size_of(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(100)
        assert tree.size == 0
        assert tree.capacity == 100
        assert tree.elements != NULL

        for i in range(100):
            binary_tree_set_offset(tree, i+1, i)

        assert tree.size == 100
        self.assertEqual(sizeof(FIXTagBinaryTree), 16)
        self.assertEqual(sizeof(FIXOffsetMap), 4)

        binary_tree_destroy(tree)

    def test_check_get_offset_valid(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        binary_tree_set_offset(tree, 1, 10)
        assert tree.size == 1

        cdef FIXTagBinaryTree * tree2 = binary_tree_shadow(<void*>tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap)*5)
        assert tree2 != NULL
        assert tree2.magic == 22906
        assert tree2.size == 1
        assert tree2.capacity == 5

        assert binary_tree_get_offset(tree2, 1) == 10

        binary_tree_destroy(tree)

    def test_check_get_offset_invalid(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        binary_tree_set_offset(tree, 1, 10)
        binary_tree_set_offset(tree, 2, 20)
        assert tree.size == 2

        assert binary_tree_shadow(<void*>tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap)*5) != NULL
        assert binary_tree_shadow(NULL, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), NULL, sizeof(FIXOffsetMap) * 5) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree)-1, tree.elements, sizeof(FIXOffsetMap) * 5) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree)+1, tree.elements, sizeof(FIXOffsetMap) * 5) == NULL
        assert binary_tree_shadow(<void *> tree, 0, tree.elements, sizeof(FIXOffsetMap) * 5) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap)-1) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap)+1) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5 + 1) == NULL
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 4) == NULL

        tree.elements[1].tag = 0
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5) == NULL

        tree.elements[0].tag = 0
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5) == NULL

        tree.capacity = 0
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5) == NULL

        tree.magic = 123424
        assert binary_tree_shadow(<void *> tree, sizeof(FIXTagBinaryTree), tree.elements, sizeof(FIXOffsetMap) * 5) == NULL

        binary_tree_destroy(tree)


    def test_check_get_offset_all_duplicates(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL
        assert binary_tree_get_offset(tree, 1) == USHRT_MAX

        assert binary_tree_set_offset(tree, 1, 10) == 0
        assert binary_tree_set_offset(tree, 2, 20) == 1
        assert binary_tree_set_offset(tree, 3, 30) == 2
        assert binary_tree_set_offset(tree, 4, 40) == 3
        assert binary_tree_set_offset(tree, 5, 50) == 4
        assert binary_tree_set_offset(tree, 1, 10) == USHRT_MAX-2
        assert binary_tree_set_offset(tree, 2, 20) == USHRT_MAX-2
        assert binary_tree_set_offset(tree, 3, 30) == USHRT_MAX-2
        assert binary_tree_set_offset(tree, 4, 40) == USHRT_MAX-2
        assert binary_tree_set_offset(tree, 5, 50) == USHRT_MAX-2


        assert binary_tree_get_offset(tree, 1) == USHRT_MAX
        assert binary_tree_get_offset(tree, 2) == USHRT_MAX
        assert binary_tree_get_offset(tree, 3) == USHRT_MAX
        assert binary_tree_get_offset(tree, 4) == USHRT_MAX
        assert binary_tree_get_offset(tree, 5) == USHRT_MAX

    def test_check_get_search(self):
        cdef FIXTagBinaryTree * tree = binary_tree_create(5)
        assert tree.size == 0
        assert tree.capacity == 5
        assert tree.elements != NULL
        assert binary_tree_get_offset(tree, 1) == USHRT_MAX

        assert binary_tree_set_offset(tree, 3, 10) == 0
        assert binary_tree_set_offset(tree, 5, 50) == 1
        assert binary_tree_set_offset(tree, 8, 100) == 2

        assert binary_tree_get_offset(tree, 2) == USHRT_MAX-1
        assert binary_tree_get_offset(tree, 3) == 10
        assert binary_tree_get_offset(tree, 4) == USHRT_MAX-1
        assert binary_tree_get_offset(tree, 5) == 50
        assert binary_tree_get_offset(tree, 6) == USHRT_MAX-1
        assert binary_tree_get_offset(tree, 8) == 100
        assert binary_tree_get_offset(tree, 9) == USHRT_MAX-1
        assert binary_tree_get_offset(tree, 100) == USHRT_MAX-1

