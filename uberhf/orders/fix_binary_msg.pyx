from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy
from libc.limits cimport USHRT_MAX
from uberhf.includes.asserts cimport cybreakpoint, cyassert

DEF FIX_MAGIC = 22093



DEF ERR_NOT_FOUND              =  0
DEF ERR_FIX_DUPLICATE_TAG      = -1
DEF ERR_FIX_TYPE_MISMATCH      = -2
DEF ERR_FIX_VALUE_TOOLONG      = -3
DEF ERR_FIX_TAG35_NOTALLOWED   = -4
DEF ERR_FIX_ZERO_TAG           = -5
DEF ERR_DATA_OVERFLOW          = -6
DEF ERR_MEMORY_ERROR           = -7

cdef class FIXTagHashMap(HashMapBase):
    """
    FIX Tag to data offset hashmap
    """

    def __cinit__(self, size_t min_capacity=64):
        self._new(sizeof(FIXOffsetMap), FIXTagHashMap.item_hash, FIXTagHashMap.item_compare, min_capacity)

    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil:
        return (<FIXOffsetMap*>a).tag - (<FIXOffsetMap*>b).tag

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil:
        return <uint64_t>(<FIXOffsetMap*>item).tag


cdef class FIXBinaryMsg:
    def __cinit__(self, char msg_type, uint16_t data_size):
        cdef uint16_t _data_size = max(data_size, 128)
        self._data = malloc(sizeof(FIXBinaryHeader) + _data_size)
        if self._data == NULL:
            raise MemoryError()
        self.values = self._data + sizeof(FIXBinaryHeader)

        self.tag_hashmap = FIXTagHashMap.__new__(FIXTagHashMap)

        self.header = <FIXBinaryHeader*>self._data
        self.header.magic_number = FIX_MAGIC
        self.header.msg_type = msg_type
        self.header.data_size = _data_size
        self.header.last_position = 0
        self.header.n_reallocs = 0
        self.header.tag_duplicates = 0

    cdef int _resize_data(self, uint16_t new_size) nogil:
        if new_size >= USHRT_MAX:
            return ERR_DATA_OVERFLOW

        self.header.data_size = new_size
        self._data = realloc(self._data, self.header.data_size)
        cyassert(self._data != NULL)
        if self._data == NULL:
            # Memory error!
            self.header = NULL
            return ERR_MEMORY_ERROR
        # We must reset pointers to values/header because self._data may be changed
        self.values = self._data + sizeof(FIXBinaryHeader)
        self.header = <FIXBinaryHeader *> self._data
        self.header.n_reallocs += 1
        return 1

    cdef int set(self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil:
        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 35:
            # MsgType must be set at constructor
            return ERR_FIX_TAG35_NOTALLOWED
        if value_size > 255:
            return ERR_FIX_VALUE_TOOLONG

        cdef int rc = 0

        cyassert(self.header.last_position <= USHRT_MAX)
        #cybreakpoint(tag == 8193)
        if USHRT_MAX-self.header.last_position <= <uint16_t>(value_size + sizeof(FIXRec)):
            return ERR_DATA_OVERFLOW

        if self.header.last_position + value_size + sizeof(FIXRec) > self.header.data_size:
            # Check buffer size and resize if needed
            rc = self._resize_data(min(USHRT_MAX, self.header.data_size * 2))
            if rc < 0:
                return rc

        cdef FIXRec * rec
        cdef FIXOffsetMap offset

        offset.tag = tag
        offset.data_offset = self.header.last_position

        if self.tag_hashmap.set(&offset) != NULL:
            offset.data_offset = USHRT_MAX
            self.header.tag_duplicates += 1
            self.tag_hashmap.set(&offset)
            return ERR_FIX_DUPLICATE_TAG

        # New value added successfully
        rec = <FIXRec*>(self.values + offset.data_offset)

        rec.tag = tag
        rec.value_type = value_type
        rec.value_len = value_size

        memcpy(self.values + offset.data_offset + sizeof(FIXRec), value, value_size)
        self.header.last_position += sizeof(FIXRec) + value_size
        return 1

    cdef int get(self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil:
        cdef FIXRec * rec
        cdef FIXOffsetMap offset
        offset.tag = tag
        offset.data_offset = USHRT_MAX
        # Fill default return values
        value[0] = NULL
        value_size[0] = 0

        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 35:
            # Getting message type
            if value_type != b'c':
                return ERR_FIX_TYPE_MISMATCH
            value[0] = &self.header.msg_type
            value_size[0] = sizeof(char)
            return 1

        cdef FIXOffsetMap * p_offset_found

        p_offset_found = <FIXOffsetMap*>self.tag_hashmap.get(&offset)
        if p_offset_found == NULL:
            # Tag not found
            return ERR_NOT_FOUND

        if p_offset_found.data_offset == USHRT_MAX:
            # Possibly duplicate tag, return as error
            return ERR_FIX_DUPLICATE_TAG

        rec = <FIXRec *> (self.values + p_offset_found.data_offset)

        if rec.value_type != value_type:
            # Type mismatch
            return ERR_FIX_TYPE_MISMATCH

        value_size[0] = rec.value_len
        value[0] = self.values + p_offset_found.data_offset + sizeof(FIXRec)
        return 1

    def __dealloc__(self):
        if self._data != NULL:
            free(self._data)
            self._data = NULL
