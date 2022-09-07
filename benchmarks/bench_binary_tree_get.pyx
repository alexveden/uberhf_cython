from libc.stdint cimport uint16_t

cdef extern from *:
    """
    /* This code is a fast implementation of ceil((x+y)/2) */
    #define avg_ceil(x, y) ( ((x+y)/2 + ((x+y) % 2 != 0) ))
    """
    uint16_t avg_ceil(uint16_t x, uint16_t y) nogil

"""
cython production build
----------------------------------------------------------------------------------------------------
SEQUENTIAL ACCESS
binary_tree_get_offset (original) speed: 580113.8013925041 iter/sec
binary_tree_get_offset_alt_wiki speed: 523579.2046517028 iter/sec
binary_tree_get_offset_last_tag_cache speed: 1304486.9980623832 iter/sec
binary_tree_get_offset (prod func) speed: 1270162.1353166697 iter/sec
py_dict speed: 251988.24380408583 iter/sec
----------------------------------------------------------------------------------------------------
RANDOM ACCESS
binary_tree_get_offset (original) speed: 446857.62745812477 iter/sec
binary_tree_get_offset_alt_wiki speed: 383399.30053438 iter/sec
binary_tree_get_offset_last_tag_cache speed: 855598.0082371348 iter/sec
binary_tree_get_offset (prod func) speed: 871947.7285058843 iter/sec
----------------------------------------------------------------------------------------------------
SAME TAG ACCESS
binary_tree_get_offset (original) speed: 733826.7146751207 iter/sec
binary_tree_get_offset_alt_wiki speed: 639134.9598550546 iter/sec
binary_tree_get_offset_last_tag_cache speed: 2332358.7145709028 iter/sec
binary_tree_get_offset (prod func) speed: 2569488.16123993 iter/sec
----------------------------------------------------------------------------------------------------
NOT FOUND
binary_tree_get_offset (original) speed: 624086.4432478116 iter/sec
binary_tree_get_offset_alt_wiki speed: 522010.07851985894 iter/sec
binary_tree_get_offset_last_tag_cache speed: 1014521.0641781983 iter/sec
binary_tree_get_offset (prod func) speed: 1114548.9380133555 iter/sec


"""



from libc.limits cimport USHRT_MAX
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from libc.stdlib cimport calloc, free, realloc, malloc
from libc.string cimport memset, memmove
from libc.math cimport ceil
cimport cython
from libc.stdint cimport uint16_t
DEF BINARY_TREE_MAGIC = 22906
DEF RESULT_ERROR =   	65535 # USHRT_MAX
DEF RESULT_NOT_FOUND = 	65534 # USHRT_MAX-1
DEF RESULT_DUPLICATE = 	65533 # USHRT_MAX-2
from uberhf.orders.fix_tag_tree cimport  *
import time

cdef uint16_t binary_tree_get_offset_original(FIXTagBinaryTree * tree, uint16_t tag) nogil:
    cdef uint16_t start_index = 0
    cdef uint16_t end_index = tree.size
    cdef uint16_t middle = 0
    cdef uint16_t element = tag
    cdef uint16_t data_offset = USHRT_MAX

    if end_index == 0 or tag == 0 or tag >= USHRT_MAX-10:
        return RESULT_ERROR

    # Try fast way
    if tree.elements[0].tag >= tag:
        if tree.elements[0].tag == tag:
            data_offset = tree.elements[0].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return RESULT_DUPLICATE
            else:
                return data_offset
        else:
            return RESULT_NOT_FOUND
    if tree.elements[tree.size-1].tag <= tag:
        if tree.elements[tree.size-1].tag == tag:
            data_offset = tree.elements[tree.size-1].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return RESULT_DUPLICATE
            else:
                return data_offset
        else:
            return RESULT_NOT_FOUND

    while start_index <= end_index:
        middle = start_index + <uint16_t>((end_index - start_index ) / 2)
        #cyassert(middle < tree.size)
        if tree.elements[middle].tag == element:
            data_offset = tree.elements[middle].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return RESULT_DUPLICATE
            else:
                return data_offset
        if tree.elements[middle].tag < element:
            start_index = middle + 1
            #cyassert(start_index < tree.size)
        else:
            end_index = middle - 1
            #cyassert(end_index < tree.size)

    return RESULT_NOT_FOUND


cdef uint16_t binary_tree_get_offset_alt_wiki(FIXTagBinaryTree * tree, uint16_t tag) nogil:
    cdef uint16_t start_index = 0
    cdef uint16_t end_index = tree.size-1
    cdef uint16_t middle = 0
    cdef uint16_t element = tag
    cdef uint16_t data_offset = USHRT_MAX

    if end_index == 0 or tag == 0 or tag >= USHRT_MAX-10:
        return RESULT_ERROR

    while start_index != end_index:
        # m := ceil((L + R) / 2)
        middle = (start_index + end_index)
        middle = <uint16_t>(middle/2) + (middle % 2 != 0)
        #cyassert(middle < tree.size)
        if tree.elements[middle].tag > element:
            end_index = middle - 1
        else:
            start_index = middle

    if tree.elements[start_index].tag == element:
        data_offset = tree.elements[start_index].data_offset
        if data_offset == USHRT_MAX:
            # Duplicate sign
            return RESULT_DUPLICATE
        else:
            return data_offset

    return RESULT_NOT_FOUND


cdef uint16_t last_tag = 0
cdef uint16_t last_tag_idx = USHRT_MAX

#@cython.cdivision(True)
cdef uint16_t binary_tree_get_offset_last_tag_cache(FIXTagBinaryTree * tree, uint16_t tag) nogil:
    global last_tag, last_tag_idx
    cdef uint16_t start_index = 0
    cdef uint16_t end_index = tree.size - 1
    cdef uint16_t middle = 0
    cdef uint16_t data_offset = USHRT_MAX

    if end_index == 0 or tag == 0 or tag >= USHRT_MAX - 10:
        return RESULT_ERROR

    if last_tag != 0:
        if last_tag == tag:
            start_index = end_index = last_tag_idx
        elif last_tag_idx < end_index and tree.elements[last_tag_idx+1].tag == tag:
            # Yep sequential next!
            start_index = end_index = last_tag_idx + 1
        elif last_tag > tag:
            end_index = last_tag_idx
        else:
            start_index = last_tag_idx

    # Try fast way
    if tree.elements[0].tag >= tag:
        if tree.elements[0].tag == tag:
            data_offset = tree.elements[0].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return RESULT_DUPLICATE
            else:
                return data_offset
        else:
            return RESULT_NOT_FOUND
    if tree.elements[tree.size - 1].tag <= tag:
        if tree.elements[tree.size - 1].tag == tag:
            data_offset = tree.elements[tree.size - 1].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return RESULT_DUPLICATE
            else:
                return data_offset
        else:
            return RESULT_NOT_FOUND

    while start_index != end_index:
        # m := ceil((L + R) / 2)
        middle = avg_ceil(start_index, end_index)
        #middle = start_index + <uint16_t> ceil((end_index - start_index) / 2)
        #cyassert(middle < tree.size)
        if tree.elements[middle].tag > tag:
            end_index = middle - 1
        else:
            start_index = middle

    if tree.elements[start_index].tag == tag:
        data_offset = tree.elements[start_index].data_offset
        last_tag = tag
        last_tag_idx = start_index
        if data_offset == USHRT_MAX:
            # Duplicate sign
            return RESULT_DUPLICATE
        else:
            return data_offset

    return RESULT_NOT_FOUND

cpdef main():
    global last_tag, last_tag_idx
    cdef int i, j
    #
    # Avg ceil test
    #
    for i in range(USHRT_MAX):
        for j in range(USHRT_MAX):
            assert avg_ceil(i, j) == ceil((i+j)/2), f'x={i} y={j} avg_ceil(x, y)={avg_ceil(i, j)} math.ceil={ceil((i+j)/2)}'

    cdef int rnd_array[100]
    rnd_array = [629, 1574, 8407, 4136, 3012, 4371, 2613, 1576, 4968, 2367, 3160,
                 9330, 2231, 6958, 4545, 5648, 2134, 1644, 4568, 4196, 9325, 1292,
                 7449, 932, 3944, 4544, 8256, 2811, 8740, 1683, 2098, 1270, 4299,
                 6629, 1537, 9760, 7299, 4444, 8096, 5058, 7178, 1512, 5625, 5444,
                 8786, 9222, 7783, 336, 4081, 2862, 1773, 3077, 2058, 8112, 7052,
                 7473, 1032, 165, 429, 5298, 2792, 6973, 1269, 7433, 5798, 8885,
                 7434, 1298, 2422, 1712, 7733, 5693, 4637, 8607, 4721, 1136, 8695,
                 7101, 3681, 6409, 5534, 1671, 1204, 4491, 2192, 3082, 9174, 7384,
                 2102, 4858, 1795, 4032, 7711, 9536, 1294, 2230, 5856, 7797, 4313,
                 5908]


    cdef FIXTagBinaryTree * tag_tree = binary_tree_create(100)
    cdef FIXTagBinaryTree * tag_tree_rnd = binary_tree_create(100)


    py_dict = {}
    # Fill sequential
    for i in range(10, 90):
        if i % 2 == 0:
            assert binary_tree_set_offset(tag_tree, i+1,  i) < USHRT_MAX-10
            py_dict[i+1] = i

    # Fill random
    for i in range(100):
        assert binary_tree_set_offset(tag_tree_rnd, rnd_array[i], rnd_array[i]) < USHRT_MAX - 10


    cdef int n_steps = 100000

    ################################################################################
    #  SEQUENTIAL
    ################################################################################
    print('-' * 100)
    print('SEQUENTIAL ACCESS')
    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            if i % 2 == 0 and i >= 10 and i < 90:
                assert binary_tree_get_offset_original(tag_tree, i+1) == i
            else:
                assert (binary_tree_get_offset_original(tag_tree, i + 1) == RESULT_NOT_FOUND)

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (original) speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            if i % 2 == 0 and i >= 10 and i < 90:
                assert binary_tree_get_offset_alt_wiki(tag_tree, i + 1) == i, i
            else:
                assert (binary_tree_get_offset_alt_wiki(tag_tree, i + 1) == RESULT_NOT_FOUND), i

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_alt_wiki speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            if i % 2 == 0 and i >= 10 and i < 90:
                assert binary_tree_get_offset_last_tag_cache(tag_tree, i + 1) == i, i
            else:
                assert (binary_tree_get_offset_last_tag_cache(tag_tree, i + 1) == RESULT_NOT_FOUND), i

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_last_tag_cache speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            if i % 2 == 0 and i >= 10 and i < 90:
                assert binary_tree_get_offset(tag_tree, i + 1) == i, i
            else:
                assert (binary_tree_get_offset(tag_tree, i + 1) == RESULT_NOT_FOUND), i

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (prod func) speed: {n_steps / duration} iter/sec')


    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            if i % 2 == 0 and i >= 10 and i < 90:
                assert py_dict[i+1] == i
            else:
                assert py_dict.get(i+1) is None

    t_end = time.time()
    duration = t_end - t_start
    print(f'py_dict speed: {n_steps / duration} iter/sec')

    ################################################################################
    #  RANDOM
    ################################################################################
    print('-' * 100)
    print('RANDOM ACCESS')
    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_original(tag_tree_rnd, rnd_array[i]) == rnd_array[i]
    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (original) speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_alt_wiki(tag_tree_rnd, rnd_array[i]) == rnd_array[i]

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_alt_wiki speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_last_tag_cache(tag_tree_rnd, rnd_array[i]) == rnd_array[i]

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_last_tag_cache speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset(tag_tree_rnd, rnd_array[i]) == rnd_array[i]

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (prod func) speed: {n_steps / duration} iter/sec')

    ################################################################################
    #  SAME TAG
    ################################################################################
    print('-' * 100)
    print('SAME TAG ACCESS')
    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_original(tag_tree, 21) < USHRT_MAX-100
    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (original) speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_alt_wiki(tag_tree, 21)< USHRT_MAX-100

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_alt_wiki speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset_last_tag_cache(tag_tree, 21)< USHRT_MAX-100
    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_last_tag_cache speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(100):
            assert binary_tree_get_offset(tag_tree, 21) < USHRT_MAX-100

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (prod func) speed: {n_steps / duration} iter/sec')

    ################################################################################
    #  NOTHING FOUND
    ################################################################################
    print('-' * 100)
    print('NOT FOUND')
    t_start = time.time()
    for j in range(n_steps):
        for i in range(1, 100):
            if i % 2 != 0:
                assert (binary_tree_get_offset_original(tag_tree, i + 1) == RESULT_NOT_FOUND), i
            else:
                assert (binary_tree_get_offset_original(tag_tree, i) == RESULT_NOT_FOUND), i
    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (original) speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    for j in range(n_steps):
        for i in range(1, 100):
            if i % 2 != 0:
                assert (binary_tree_get_offset_alt_wiki(tag_tree, i + 1) == RESULT_NOT_FOUND), i
            else:
                assert (binary_tree_get_offset_alt_wiki(tag_tree, i) == RESULT_NOT_FOUND), i

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_alt_wiki speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(1, 100):
            if i % 2 != 0:
                assert (binary_tree_get_offset_last_tag_cache(tag_tree, i + 1) == RESULT_NOT_FOUND), i
            else:
                assert (binary_tree_get_offset_last_tag_cache(tag_tree, i) == RESULT_NOT_FOUND), i
    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset_last_tag_cache speed: {n_steps / duration} iter/sec')

    t_start = time.time()
    last_tag = 0
    last_tag_idx = USHRT_MAX
    for j in range(n_steps):
        for i in range(1, 100):
            if i % 2 != 0:
                assert (binary_tree_get_offset(tag_tree, i + 1) == RESULT_NOT_FOUND), i
            else:
                assert (binary_tree_get_offset(tag_tree, i) == RESULT_NOT_FOUND), i

    t_end = time.time()
    duration = t_end - t_start
    print(f'binary_tree_get_offset (prod func) speed: {n_steps / duration} iter/sec')

