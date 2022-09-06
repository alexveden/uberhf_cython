from uberhf.orders.fix_binary_msg cimport FIXOffsetMap, FIXTagHashMap
from uberhf.includes.utils cimport timer_nsnow, timedelta_ns, TIMEDELTA_SEC, random_int
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from libc.stdlib cimport calloc, free, realloc, malloc
from libc.string cimport memset, memmove
from libc.stdint cimport uint16_t
from libc.limits cimport USHRT_MAX
from bisect import bisect_left
from uberhf.orders.fix_tag_tree cimport *


cdef tree_make_sequential(int n_elements):
    cdef FIXTagBinaryTree *tree = binary_tree_create(100)
    cdef FIXOffsetMap offset
    cdef uint16_t tag_offset
    cdef int i =0

    #cyassert(tree.capacity[0] == 64)
    #cyassert(tree.size[0] == 0)

    for i in range(n_elements-1, -1, -1):
    #for i in range(n_elements):
        binary_tree_set_offset(tree, i+1, i+3)

    cyassert(tree.size == n_elements)

    # for i in range(n_elements):
    #     cyassert(tree.elements[i].tag == i+1)
    #     cyassert(tree.elements[i].data_offset == i + 3)

    for i in range(n_elements):
        tag_offset = binary_tree_get_offset(tree, i + 1)
        cyassert(tag_offset != USHRT_MAX)
        cyassert(tag_offset == i+3)

    binary_tree_destroy(tree)

cdef tree_make_random(int n_elements):
    cdef FIXTagBinaryTree* tree = binary_tree_create(64)
    cdef FIXOffsetMap offset
    cdef uint16_t tag_index
    cdef int i =0
    cdef int tag = 0
    cdef int n_unique = 0

    for i in range(n_elements):
        tag = random_int(1, n_elements)
        if binary_tree_set_offset(tree, tag, tag + 3) != USHRT_MAX:
            n_unique += 1

    cyassert(tree.size == n_unique)

    for i in range(n_elements):
        tag = random_int(1, n_elements)
        tag_index = binary_tree_get_offset(tree, tag)
        if tag_index != USHRT_MAX:
            cyassert(tag_index == tag + 3)

    binary_tree_destroy(tree)

cdef hashmap_make_sequential(int n_elements):
    cdef FIXTagHashMap hm = FIXTagHashMap.__new__(FIXTagHashMap)
    cdef FIXOffsetMap offset
    cdef FIXOffsetMap * p_offset
    cdef int i =0


    for i in range(n_elements):
        offset.tag = i + 1
        offset.data_offset = i+3
        cyassert(hm.set(&offset) == NULL)

    for i in range(n_elements):
        offset.tag = i + 1
        offset.data_offset = 0

        p_offset = <FIXOffsetMap *>hm.get(&offset)
        cyassert(p_offset != NULL)
        cyassert(p_offset.tag == i+1)
        cyassert(p_offset.data_offset == i + 3)

cdef hashmap_make_random(int n_elements):
    cdef FIXTagHashMap hm = FIXTagHashMap.__new__(FIXTagHashMap)
    cdef FIXOffsetMap offset
    cdef FIXOffsetMap * p_offset
    cdef int i = 0
    cdef int tag = 0
    cdef int n_unique = 0

    for i in range(n_elements):
        offset.tag = random_int(1, n_elements)
        offset.data_offset = offset.tag + 3
        if hm.set(&offset) == NULL:
            n_unique += 1

    for i in range(n_elements):
        offset.tag = random_int(1, n_elements)
        offset.data_offset = 0

        p_offset = <FIXOffsetMap *> hm.get(&offset)
        if p_offset != NULL:
            cyassert(p_offset.tag == offset.tag)
            cyassert(p_offset.data_offset == offset.tag + 3)


def python_make_sequential(n_elements):
    hm = {}
    for i in range(n_elements):
        hm[i+1] = i+1

    for i in range(n_elements):
        hm[i+1]

cpdef main():
    cdef int msg_size = 0
    cdef int n_steps = 10000
    cdef long t_start
    cdef long t_end
    cdef double duration
    cdef int i = 0

    msg_size = 100
    print('-'*100)
    print(f'SEQUENTIAL ACCESS {msg_size} tags/msg')
    t_start = timer_nsnow()
    for i in range(n_steps):
        hashmap_make_sequential(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'Hashmap speed: {n_steps/duration} msg/sec')


    t_start = timer_nsnow()
    for i in range(n_steps):
        tree_make_sequential(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'Binary tree  speed: {n_steps/duration} msg/sec')


    t_start = timer_nsnow()
    for i in range(n_steps):
        python_make_sequential(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'Python dict speed: {n_steps/duration} msg/sec')

    print('-' * 100)
    print(f'RANDOM ACCESS {msg_size} tags/msg')
    t_start = timer_nsnow()
    for i in range(n_steps):
        hashmap_make_random(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'Hashmap speed: {n_steps/duration} msg/sec')
    #
    t_start = timer_nsnow()
    for i in range(n_steps):
        tree_make_random(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'Binary tree  speed: {n_steps/duration} msg/sec')

    print(f'NO OBJ CREATION {msg_size} tags/msg')
    cdef FIXTagBinaryTree * tree = binary_tree_create(64)
    cdef FIXOffsetMap offset
    cdef uint16_t tag_index
    cdef int tag = 0
    cdef int n_unique = 0

    t_start = timer_nsnow()

    for k in range(n_steps):
        tree.size = 0

        for i in range(msg_size):
            binary_tree_set_offset(tree, i + 1, i + 3)
        cyassert(tree.size == msg_size)
        for i in range(msg_size):
            tag_offset = binary_tree_get_offset(tree, i + 1)
            cyassert(tag_offset != USHRT_MAX)
            cyassert(tag_offset == i+3)


    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    binary_tree_destroy(tree)
    print(f'Binary tree speed: {n_steps / duration} msg/sec')