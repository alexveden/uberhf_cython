from libc.limits cimport USHRT_MAX
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from libc.stdlib cimport calloc, free, realloc, malloc
from libc.string cimport memset, memmove
cimport cython
from libc.stdint cimport uint16_t

cdef extern from *:
    """
    /*
        This code is a ultra-fast implementation of ceil((x+y)/2)
    */
    #define avg_ceil(x, y) ( ((x+y)/2 + ((x+y) % 2 != 0) ))
    #define avg(x, y) ( (x+y)/2 )
    """
    uint16_t avg_ceil(uint16_t x, uint16_t y) nogil
    uint16_t avg(uint16_t x, uint16_t y) nogil

DEF BINARY_TREE_MAGIC = 22906
DEF RESULT_ERROR =   	65535 # USHRT_MAX
DEF RESULT_NOT_FOUND = 	65534 # USHRT_MAX-1
DEF RESULT_DUPLICATE = 	65533 # USHRT_MAX-2

cdef FIXTagBinaryTree * binary_tree_create(uint16_t initial_capacity) nogil:
    cyassert(initial_capacity > 0)

    cdef FIXTagBinaryTree * tree = <FIXTagBinaryTree*> malloc(sizeof(FIXTagBinaryTree))
    cyassert(tree != NULL)

    tree.magic = BINARY_TREE_MAGIC
    tree.size = 0
    tree.last_tag = 0
    tree.last_tag_idx = USHRT_MAX
    tree.capacity = initial_capacity
    tree.elements = <FIXOffsetMap*> calloc(tree.capacity, sizeof(FIXOffsetMap))
    cyassert(tree.elements != NULL)
    return tree


@cython.cdivision(True)
cdef FIXTagBinaryTree * binary_tree_shadow(void * data, size_t data_size, void * elements, size_t el_capacity_size) nogil:
    if data == NULL or elements == NULL:
        return NULL
    if data_size != sizeof(FIXTagBinaryTree):
        return NULL
    if el_capacity_size < sizeof(FIXOffsetMap) or el_capacity_size > (USHRT_MAX - 1)*sizeof(FIXOffsetMap) or el_capacity_size % sizeof(FIXOffsetMap) != 0:
        # Element data size mismatch, overflow or not aligned to size of FIXOffsetMap
        return NULL

    cdef FIXTagBinaryTree * tree = <FIXTagBinaryTree*>data

    if tree.magic != BINARY_TREE_MAGIC:
        return NULL
    if tree.capacity <= 0:
        return NULL
    if tree.capacity * sizeof(FIXOffsetMap) != el_capacity_size:
        return NULL

    tree.elements = <FIXOffsetMap*>elements
    tree.last_tag = 0
    tree.last_tag_idx = USHRT_MAX
    if tree.size > 0:
        if tree.elements[0].tag == 0 or tree.elements[tree.size-1].tag == 0:
            # Probe first and last element (because zero tag is not allowed!)
            return NULL
    return tree

cdef void binary_tree_destroy(FIXTagBinaryTree * tree) nogil:
    if tree != NULL:
        if tree.elements != NULL:
            free(tree.elements)
            tree.elements = NULL
        free(tree)

cdef bint binary_tree_resize(FIXTagBinaryTree * tree, size_t new_capacity) nogil:
    cdef size_t old_capacity = tree.capacity
    new_capacity = min(USHRT_MAX-1, new_capacity)

    if old_capacity >= <size_t>USHRT_MAX-1 and new_capacity >= <size_t>USHRT_MAX-1:
        return False

    #cdef size_t old_size = sizeof(FIXOffsetMap) * old_capacity
    cyassert(tree.elements != NULL)
    cyassert(new_capacity > old_capacity)
    cyassert(new_capacity <= USHRT_MAX)
    cdef size_t new_size = sizeof(FIXOffsetMap) * min(USHRT_MAX, new_capacity)

    cdef void* new_alloc = realloc(<void*>tree.elements, new_size)
    if new_alloc == NULL:
        # Out of memory
        return False

    tree.elements = <FIXOffsetMap *>new_alloc
    tree.capacity = new_capacity
    return True

cdef uint16_t binary_tree_set_offset(FIXTagBinaryTree * tree, uint16_t tag, uint16_t tag_offset) nogil:
    if tag == 0 or tag > USHRT_MAX-10 or tag_offset >= USHRT_MAX-10:
        return RESULT_ERROR

    cdef uint16_t tree_size = tree.size
    cdef uint16_t lo, hi, mid

    if tree_size+1 > tree.capacity:
        if not binary_tree_resize(tree, tree.capacity + 10):
            return RESULT_ERROR

    if tree_size == 0:
        tree.elements[0].tag = tag
        tree.elements[0].data_offset = tag_offset
        tree.size += 1
        tree.last_tag = tag
        tree.last_tag_idx = 0
        return 0
    else:
        if tree.elements[tree_size-1].tag < tag:
            # Tag > upper bound
            tree.elements[tree_size].tag = tag
            tree.elements[tree_size].data_offset = tag_offset
            tree.size += 1
            tree.last_tag = tag
            tree.last_tag_idx = tree.size-1
            return tree.size-1
        elif tree.elements[0].tag > tag:
            # Tag < lower bound
            memmove(&tree.elements[1], &tree.elements[0], sizeof(FIXOffsetMap) * tree_size)
            tree.elements[0].tag = tag
            tree.elements[0].data_offset = tag_offset
            tree.size += 1
            tree.last_tag = tag
            tree.last_tag_idx = 0
            return 0
        else:
            # Worst case scenario, some random index inside bounds
            lo = 0
            hi = tree_size
            while lo < hi:
                #mid = <uint16_t>((lo + hi) / 2)
                mid = avg(lo, hi)
                if tree.elements[mid].tag < tag:
                    lo = mid + 1
                else:
                    hi = mid
            if tree.elements[lo].tag == tag:
                # It's strictly forbidden to have duplicate fix messages
                #    this will lead to a whole message corruption status!
                tree.elements[lo].data_offset = USHRT_MAX
                return RESULT_DUPLICATE

            tree.last_tag = tag
            tree.last_tag_idx = lo

            cyassert(tree.elements[lo].tag > tag)
            last_tag = tree.elements[tree_size-1].tag
            memmove(&tree.elements[lo+1], &tree.elements[lo], sizeof(FIXOffsetMap) * (tree_size-lo))
            tree.elements[lo].tag = tag
            tree.elements[lo].data_offset = tag_offset
            tree.size += 1
            cyassert(last_tag == tree.elements[tree_size].tag)
            return lo


cdef uint16_t binary_tree_get_offset(FIXTagBinaryTree * tree, uint16_t tag) nogil:
    if tree.size == 0 or tag == 0 or tag >= USHRT_MAX - 10:
        return RESULT_ERROR

    cdef uint16_t start_index = 0
    cdef uint16_t end_index = tree.size - 1
    cdef uint16_t middle = 0
    cdef uint16_t data_offset = USHRT_MAX

    if tree.last_tag != 0:
        if tree.last_tag == tag:
            # The same tag, possibly a group tag
            start_index = end_index = tree.last_tag_idx
        elif tree.last_tag_idx < end_index and tree.elements[tree.last_tag_idx+1].tag == tag:
            # Yep sequential next!
            start_index = end_index = tree.last_tag_idx + 1
        elif tree.last_tag > tag:
            end_index = tree.last_tag_idx
        else:
            start_index = tree.last_tag_idx
    #
    # Try fast way check boundaries
    if tree.elements[0].tag > tag:
        return RESULT_NOT_FOUND
    if tree.elements[tree.size - 1].tag < tag:
        return RESULT_NOT_FOUND

    while start_index != end_index:
        # m := ceil((L + R) / 2)
        middle = avg_ceil(start_index, end_index)

        if tree.elements[middle].tag > tag:
            end_index = middle - 1
        else:
            start_index = middle

    if tree.elements[start_index].tag == tag:
        tree.last_tag = tag
        tree.last_tag_idx = start_index
        data_offset = tree.elements[start_index].data_offset
        if data_offset == USHRT_MAX:
            # Duplicate sign
            return RESULT_DUPLICATE
        else:
            return data_offset

    return RESULT_NOT_FOUND
