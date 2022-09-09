from libc.stdlib cimport malloc, realloc, free
from libc.stdio cimport printf
from libc.string cimport memcpy, memset, strlen, memmove
from libc.limits cimport USHRT_MAX, UCHAR_MAX
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from uberhf.includes.utils cimport strlcpy
from uberhf.orders.fix_tag_tree cimport binary_tree_destroy, binary_tree_create, binary_tree_set_offset, binary_tree_get_offset
DEF FIX_MAGIC = 22093

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

# RetCodes for FIXMsg._get_tag_index/FIXMsg._set_tag_index
DEF TAG_ERROR =      	65535 # USHRT_MAX
DEF TAG_NOT_FOUND = 	65534 # USHRT_MAX-1
DEF TAG_DUPLICATE = 	65533 # USHRT_MAX-2
DEF TAG_NEED_RESIZE = 	65532 # USHRT_MAX-3

DEF ERR_NOT_FOUND              =  0
DEF ERR_FIX_DUPLICATE_TAG      = -1
DEF ERR_FIX_TYPE_MISMATCH      = -2
DEF ERR_FIX_VALUE_TOOLONG      = -3
DEF ERR_FIX_NOT_ALLOWED        = -4
DEF ERR_FIX_ZERO_TAG           = -5
DEF ERR_DATA_OVERFLOW          = -6
DEF ERR_MEMORY_ERROR           = -7
DEF ERR_GROUP_NOT_FINISHED     = -8
DEF ERR_GROUP_EMPTY            = -9
DEF ERR_GROUP_DUPLICATE_TAG    = -10
DEF ERR_GROUP_NOT_STARTED      = -11
DEF ERR_GROUP_NOT_MATCH        = -12
DEF ERR_GROUP_TOO_MANY         = -13
DEF ERR_GROUP_START_TAG_EXPECTED  = -14
DEF ERR_GROUP_EL_OVERFLOW      = -15
DEF ERR_GROUP_TAG_NOT_INGROUP  = -16
DEF ERR_GROUP_NOT_COMPLETED    = -17
DEF ERR_GROUP_TAG_WRONG_ORDER  = -18
DEF ERR_GROUP_CORRUPTED        = -19
DEF ERR_UNEXPECTED_TYPE_SIZE   = -20
DEF ERR_TAG_RESIZE_REQUIRED    = -21
DEF ERR_DATA_RESIZE_REQUIRED   = -22
DEF ERR_DATA_READ_ONLY         = -23


cdef class FIXMsg:
    @staticmethod
    cdef FIXMsgStruct * create(char msg_type, uint16_t data_size, uint8_t tag_tree_capacity) nogil:
        if data_size == 0:
            return NULL
        if tag_tree_capacity == 0:
            return NULL
        cdef FIXMsgStruct * self = <FIXMsgStruct *> malloc(_calc_data_size(data_size, tag_tree_capacity))                                          # Value storage size

        if self == NULL:
            return NULL
        # Tag index pointer
        self.tags = _calc_offset_tags(self)

        self.open_group = NULL
        self.header.magic_number = FIX_MAGIC
        self.header.msg_type = msg_type
        self.header.data_size = data_size
        self.header.last_position = 0
        self.header.n_reallocs = 0
        self.header.tag_errors = 0
        self.header.is_read_only = 0

        # Initialize tags
        self.header.tags_count = 0
        self.header.tags_capacity = tag_tree_capacity
        self.header.tags_last = 0
        self.header.tags_last_idx = UCHAR_MAX

        # Tag data pointer (MUST BE AFTER HEADER INITIALIZED!)
        self.values = _calc_offset_values(self)


        # Put magic number between tag index and values starting point, to make sure that resizing of tags doesn't affect the integrity
        cdef uint16_t * magic_middle = _calc_offset_magic_middle(self)
        magic_middle[0] = FIX_MAGIC

        cdef uint16_t * magic_end = _calc_offset_magic_end(self)
        magic_end[0] = FIX_MAGIC

        return self

    @staticmethod
    cdef void destroy(FIXMsgStruct * self) nogil:
        if self != NULL:
            free(self)

    @staticmethod
    cdef bint is_valid(FIXMsgStruct * self) nogil:
        """
        Check is binary message is valid

        Validity criteria
        - FIX_MAGIC in header
        - no data overflow
        - no tag errors
        - no currently open groups
        - valid msg type > 0
        :return:
        """
        cdef uint16_t * magic_middle = _calc_offset_magic_middle(self)
        cdef uint16_t * magic_end = _calc_offset_magic_end(self)

        return (
                self.header.magic_number == FIX_MAGIC and
                self.header.last_position < USHRT_MAX and
                self.header.tag_errors == 0 and
                self.open_group == NULL and
                self.header.msg_type > 0 and
                self.header.tags_capacity > 0 and
                magic_middle[0] == FIX_MAGIC and
                magic_end[0] == FIX_MAGIC
        )

    @staticmethod
    cdef uint16_t _set_tag_offset(FIXMsgStruct * self, uint16_t tag, uint16_t tag_offset) nogil:
        if tag == 0 or tag > USHRT_MAX - 10 or tag_offset >= USHRT_MAX - 10:
            return TAG_ERROR
        if self.header.is_read_only:
            return TAG_ERROR

        cdef uint16_t tree_size = self.header.tags_count
        cdef uint16_t lo, hi, mid

        if tree_size + 1 > self.header.tags_capacity:
            # This is typically unexpected behavior, and should be handled by has_capacity()
            self.header.tag_errors += 1
            return TAG_NEED_RESIZE

        if tree_size == 0:
            self.tags[0].tag = tag
            self.tags[0].data_offset = tag_offset
            self.header.tags_count += 1
            self.header.tags_last = tag
            self.header.tags_last_idx = 0
            return 0
        else:
            if self.tags[self.header.tags_count - 1].tag < tag:
                # Tag > upper bound
                self.tags[tree_size].tag = tag
                self.tags[tree_size].data_offset = tag_offset
                self.header.tags_count += 1
                self.header.tags_last = tag
                self.header.tags_last_idx = self.header.tags_count - 1
                return self.header.tags_count - 1
            elif self.tags[0].tag > tag:
                # Tag < lower bound
                memmove(&self.tags[1], &self.tags[0], sizeof(FIXOffsetMap) * tree_size)
                self.tags[0].tag = tag
                self.tags[0].data_offset = tag_offset
                self.header.tags_count += 1
                self.header.tags_last = tag
                self.header.tags_last_idx = 0
                return 0
            else:
                # Worst case scenario, some random index inside bounds
                lo = 0
                hi = tree_size
                while lo < hi:
                    #mid = <uint16_t>((lo + hi) / 2)
                    mid = avg(lo, hi)
                    if self.tags[mid].tag < tag:
                        lo = mid + 1
                    else:
                        hi = mid
                if self.tags[lo].tag == tag:
                    # It's strictly forbidden to have duplicate fix messages
                    #    this will lead to a whole message corruption status!
                    self.tags[lo].data_offset = USHRT_MAX
                    self.header.tag_errors += 1
                    return TAG_DUPLICATE

                self.header.tags_last  = tag
                self.header.tags_last_idx = lo

                cyassert(self.tags[lo].tag > tag)
                last_tag = self.tags[tree_size - 1].tag
                memmove(&self.tags[lo + 1], &self.tags[lo], sizeof(FIXOffsetMap) * (tree_size - lo))
                self.tags[lo].tag = tag
                self.tags[lo].data_offset = tag_offset
                self.header.tags_count += 1
                cyassert(last_tag == self.tags[tree_size].tag)
                return lo

    @staticmethod
    cdef uint16_t _get_tag_offset(FIXMsgStruct * self, uint16_t tag) nogil:
        if self.header.tags_count == 0 or tag == 0 or tag >= USHRT_MAX - 10:
            return TAG_ERROR

        cdef uint16_t start_index = 0
        cdef uint16_t end_index = self.header.tags_count - 1
        cdef uint16_t middle = 0
        cdef uint16_t data_offset = USHRT_MAX

        if self.header.tags_last != 0:
            if self.header.tags_last == tag:
                # The same tag, possibly a group tag
                start_index = end_index = self.header.tags_last_idx
            elif self.header.tags_last_idx < end_index and self.tags[self.header.tags_last_idx + 1].tag == tag:
                # Yep sequential next!
                start_index = end_index = self.header.tags_last_idx + 1
            elif self.header.tags_last > tag:
                end_index = self.header.tags_last_idx
            else:
                start_index = self.header.tags_last_idx
        #
        # Try fast way check boundaries
        if self.tags[0].tag > tag:
            return TAG_NOT_FOUND
        if self.tags[self.header.tags_count - 1].tag < tag:
            return TAG_NOT_FOUND

        while start_index != end_index:
            # m := ceil((L + R) / 2)
            middle = avg_ceil(start_index, end_index)

            if self.tags[middle].tag > tag:
                end_index = middle - 1
            else:
                start_index = middle

        if self.tags[start_index].tag == tag:
            if not self.header.is_read_only:
                self.header.tags_last = tag
                self.header.tags_last_idx = start_index
            data_offset = self.tags[start_index].data_offset
            if data_offset == USHRT_MAX:
                # Duplicate sign
                return TAG_DUPLICATE
            else:
                return data_offset

        return TAG_NOT_FOUND

    @staticmethod
    cdef bint has_capacity(FIXMsgStruct * self, uint16_t new_rec_size) nogil:
        cyassert(new_rec_size > 0)

        if self.header.tags_count == UCHAR_MAX:
            return False

        if self.header.tags_count + 1 > self.header.tags_capacity:
            return False

        if <uint16_t> (USHRT_MAX - self.header.last_position) <= new_rec_size:
            return False

        return self.header.last_position + new_rec_size <= self.header.data_size

    @staticmethod
    cdef FIXMsgStruct * resize(FIXMsgStruct * self, uint8_t add_tags, uint16_t add_values_size) nogil:
        """
        Reallocate memory for binary FIX data container, and return new pointer to it
        
        Read only containers not allowed to resize. Invalid containers not allowed to resize!
        
        On error this function will return NULL, but old pointer will remain untouched!
                
        :param add_tags: number of extra tags to add [0;255]
        :param add_values_size: extra bytes of capacity for binary tag data
        :return: NULL on error, valid FIXMsgStruct * pointer on success
        """
        if self.header.is_read_only:
            return NULL

        if not FIXMsg.is_valid(self):
            return NULL


        if <uint16_t> (USHRT_MAX - self.header.data_size) <= add_values_size:
            self.header.last_position = USHRT_MAX
            return NULL
        if (UCHAR_MAX - self.header.tags_capacity) <= add_tags:
            self.header.last_position = USHRT_MAX
            return NULL

        # Resize to extra_size + approx 10 double tags room
        cdef size_t new_size = self.header.data_size + add_values_size
        cdef uint8_t new_tags = self.header.tags_capacity + add_tags

        # Only new_size increase allowed!
        cyassert(new_size >= self.header.data_size)
        cyassert(new_tags >= self.header.tags_capacity)

        cdef int open_group_offset = -1
        cdef int open_group_tag = -1
        if self.open_group != NULL:
            cyassert(0)  # TODO: fix this later
            # We have open group, keep offset between group and self.values if main pointer will be changed
            open_group_offset = <void *> self.open_group - self.values
            open_group_tag = self.open_group.fix_rec.tag

        cdef int old_magic_middle_offset = (<void*>_calc_offset_magic_middle(self)) - (<void*>self)

        # From `man 3 realloc`
        #    If realloc() fails, the original block is left untouched; it is not freed or moved
        cdef FIXMsgStruct * new_self = <FIXMsgStruct*>realloc(self, _calc_data_size(new_size, new_tags))
        if new_self == NULL:
            # MEMORY ERROR
            # On failure keep data untouched, but prevent any updates
            self.header.last_position = USHRT_MAX
            return NULL

        new_self.tags = _calc_offset_tags(new_self)

        if add_tags > 0:
            # Tags capacity has changed let's move memory
            memmove(<void*> new_self + old_magic_middle_offset + add_tags*sizeof(FIXOffsetMap),
                    <void*> new_self + old_magic_middle_offset,
                    new_size + sizeof(uint16_t)*2 , # Include also size of 2 magic fields
                    )

        new_self.values = _calc_offset_values(new_self)
        new_self.header.data_size = <uint16_t> new_size
        new_self.header.n_reallocs += 1
        new_self.header.tags_capacity = new_tags

        cdef uint16_t * magic_middle = _calc_offset_magic_middle(new_self)
        cdef uint16_t * magic_end = _calc_offset_magic_end(new_self)

        # Magic middle should be in place
        cyassert(magic_middle[0] == FIX_MAGIC)

        # Make sure after tags capacity change without value size the magic_end is valid
        cyassert(add_values_size != 0 or magic_end[0] == FIX_MAGIC)

        # Set magic end because of data resize
        magic_end[0] = FIX_MAGIC

        if self.open_group != NULL:
            cyassert(0)  # TODO: fix this later
            # self.open_group = <GroupRec *> (self.values + open_group_offset)
            # cyassert(self.open_group.fix_rec.tag == open_group_tag)
            # cyassert(self.open_group.fix_rec.value_type == b'\x07')

        # Message must remain valid after resize (i.e. magic numbers are in place)
        cyassert(FIXMsg.is_valid(new_self))

        return new_self


    @staticmethod
    cdef int set(FIXMsgStruct * self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil:
        """
        Set generic FIX tag to a value

        Example:
            cdef int i = 123
            cdef double f = 8907.889
            cdef char c = b'V'
            cdef char * s = b'my fancy string'

            m.set(1, &i, sizeof(int), b'i')
            m.set(2, &f, sizeof(double), b'f')
            m.set(3, &c, sizeof(char), b'c')
            m.set(4, s, strlen(s)+1, b's')

        :param tag:  tag number, except 0 and 35
        :param value: tag value generic buffer
        :param value_size: tag value size, char* must include \0 char i.e. strlen(s)+1
        :param value_type: tag value type (indication for get() method, which must be aware of this type),
                           types b'\x07' and '\x00' are not allowed, and reserved!
        :return: negative on error, positive on success
        """
        if self.open_group != NULL:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_FINISHED

        if value_type == b'\x07' or value_type == b'\x00':
            self.header.tag_errors += 1
            return ERR_FIX_NOT_ALLOWED

        if tag == 0:
            self.header.tag_errors += 1
            return ERR_FIX_ZERO_TAG
        if tag == 35:
            self.header.tag_errors += 1
            # MsgType must be set at constructor
            return ERR_FIX_NOT_ALLOWED
        if value_size == 0:
            self.header.tag_errors += 1
            return ERR_UNEXPECTED_TYPE_SIZE
        if value_size > 1024:
            # Set message as invalid, because we have failed to add new tag
            self.header.tag_errors += 1
            return ERR_FIX_VALUE_TOOLONG

        cdef int rc = 0
        cdef uint16_t last_position = self.header.last_position

        if FIXMsg.has_capacity(self, value_size + sizeof(FIXRec)) == 0:
            return ERR_DATA_RESIZE_REQUIRED

        cdef FIXRec * rec

        cdef uint16_t _data_offset_idx =  FIXMsg._set_tag_offset(self, tag, last_position)

        if _data_offset_idx  >= USHRT_MAX-10:

            if _data_offset_idx == USHRT_MAX-2: #RESULT_DUPLICATE = 	65533 # USHRT_MAX-2
                # Duplicate or error
                return ERR_FIX_DUPLICATE_TAG
            elif _data_offset_idx == USHRT_MAX-3:  # TAG_NEED_RESIZE
                return ERR_TAG_RESIZE_REQUIRED
            else:
                # Other generic error, typically oveflow
                return ERR_DATA_OVERFLOW

        # New value added successfully
        rec = <FIXRec *> (self.values + last_position)

        rec.tag = tag
        rec.value_type = value_type
        rec.value_len = value_size
        self.header.last_position += sizeof(FIXRec) + value_size

        cdef char * str_dest = NULL
        if value_type == b's':
            str_dest = <char *> (self.values + last_position + sizeof(FIXRec))
            rc = strlcpy(str_dest, <char *> value, value_size)
            if rc != value_size - 1:
                # Highly likely - argument (char* value) passed as (&value)
                str_dest[0] = b'\0'  # Make this string empty
                self.header.tag_errors += 1
                return ERR_UNEXPECTED_TYPE_SIZE
        else:
            memcpy(self.values + last_position + sizeof(FIXRec), <char *> value, value_size)

        return 1

    @staticmethod
    cdef int get(FIXMsgStruct * self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil:
        """
        Get generic tag value by reference

        Example:
            cdef int value = 123
            m.set(11, &value, sizeof(int), b'i')

            cdef void * p_value = NULL
            cdef uint16_t p_size = 0
            cdef result = 0
            if m.get(11, &p_value, &p_size, b'i') > 0:
                result = (<int*>p_value)[0]

        :param tag: tag number, 0 not allowed, when 35 returns FIXBinaryMsg.msg_type (as char)
        :param value: pointer to void*  (NULL on error)
        :param value_size: pointer to int (zero on error)
        :param value_type: the same char value as given in set method
        :return: positive on success, negative on error, zero if not found
        """
        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED
        if value_type == b'\x07' or value_type == b'\x00':
            return ERR_FIX_NOT_ALLOWED

        cdef FIXRec * rec
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

        cdef uint16_t data_offset = FIXMsg._get_tag_offset(self, tag)
        if data_offset >= USHRT_MAX-10:
            if data_offset == USHRT_MAX-1:
                # Tag not found
                return ERR_NOT_FOUND
            if data_offset == USHRT_MAX-2:
                # Possibly duplicate tag
                return ERR_FIX_DUPLICATE_TAG
            # Likely tag number is too high, other generic error, we must stop anyway
            return ERR_DATA_OVERFLOW

        rec = <FIXRec *> (self.values + data_offset)

        if rec.value_type != value_type:
            # Type mismatch
            return ERR_FIX_TYPE_MISMATCH

        value_size[0] = rec.value_len
        value[0] = self.values + data_offset + sizeof(FIXRec)
        return 1

