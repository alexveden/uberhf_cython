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
DEF ERR_RESIZE_REQUIRED        = -21
DEF ERR_DATA_READ_ONLY         = -22


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
        # Tag data pointer
        self.values = _calc_offset_values(self)
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

        # Tag index pointer (MUST BE AFTER HEADER INITIALIZED!)
        self.tags = _calc_offset_tags(self)

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
    cdef int get_last_error(FIXMsgStruct * self) nogil:
        """
        Last error returned by primitive type get operation
        :return: 
        """
        return self.header.last_error

    @staticmethod
    cdef const char * get_last_error_str(int e) nogil:
        """
        Return test error for each of error codes returned by get/set functions

        :param e: if > 0 - not an error! 
        :return: char*
        """
        if e > 0:
            return b'No error'
        elif e == ERR_NOT_FOUND:
            return b'Not found'
        elif e == ERR_FIX_DUPLICATE_TAG:
            return b'Duplicated tag'
        elif e == ERR_FIX_TYPE_MISMATCH:
            return b'Tag type mismatch'
        elif e == ERR_FIX_VALUE_TOOLONG:
            return b'Value size exceeds 1024 limit'
        elif e == ERR_FIX_NOT_ALLOWED:
            return b'FIX(35) tag or type value is not allowed'
        elif e == ERR_FIX_ZERO_TAG:
            return b'FIX tag=0 is not allowed'
        elif e == ERR_DATA_OVERFLOW:
            return b'FIX tag>=65525 or message capacity overflow'
        elif e == ERR_MEMORY_ERROR:
            return b'System memory error when resizing the message'
        elif e == ERR_GROUP_NOT_FINISHED:
            return b'You must finish the started group before using other methods'
        elif e == ERR_GROUP_EMPTY:
            return b'Group with zero members are not allowed'
        elif e == ERR_GROUP_DUPLICATE_TAG:
            return b'Group member tag is a duplicate with other tags added to message'
        elif e == ERR_GROUP_NOT_STARTED:
            return b'You must call group_start() before adding group members'
        elif e == ERR_GROUP_NOT_MATCH:
            return b'group_tag must match to the tag of the group_start()'
        elif e == ERR_GROUP_TOO_MANY:
            return b'Too many tags in the group, max 127 allowed'
        elif e == ERR_GROUP_START_TAG_EXPECTED:
            return b'You must always add the first group item with the first tag in the group tag list'
        elif e == ERR_GROUP_EL_OVERFLOW:
            return b'Group element is out of bounds, given at group_start()'
        elif e == ERR_GROUP_TAG_NOT_INGROUP:
            return b'Group member `tag` in not in tag list at group_start()'
        elif e == ERR_GROUP_NOT_COMPLETED:
            return b'Trying to finish group with incomplete elements count added, as expected at group_start()'
        elif e == ERR_GROUP_TAG_WRONG_ORDER:
            return b'You must add group tags in the same order as tag groups at group_start()'
        elif e == ERR_GROUP_CORRUPTED:
            return b'Group data is corrupted'
        elif e == ERR_UNEXPECTED_TYPE_SIZE:
            return b'Tag actual value or size does not match expected type size/value boundaries'
        elif e == ERR_RESIZE_REQUIRED:
            return b'Message is out of tag/data capacity, you need to call FIXMsg.resize(...) or increase initial capacity'
        elif e == ERR_DATA_READ_ONLY:
            return b'Message is read-only'

        return b'unknown error code'

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
    cdef int has_capacity(FIXMsgStruct * self, uint8_t add_tags, uint16_t new_rec_size) nogil:
        """
        Checks is FIXMsgStruct * self has a capacity for addint extra `add_tags` and `new_rec_size` of data
        :param self: 
        :param add_tags: number of tags adding (allowed to be 0, for example group members data)
        :param new_rec_size: number of bytes (including sizeof(FIXRec)) 
        :return: 1 on success, <= 0 on error
        """
        cyassert(new_rec_size > 0)

        if self.header.tags_count == UCHAR_MAX:
            return ERR_DATA_OVERFLOW

        if add_tags > 0 and UCHAR_MAX-self.header.tags_count < add_tags:
            return ERR_DATA_OVERFLOW

        if (USHRT_MAX - self.header.last_position) < new_rec_size:
            return ERR_DATA_OVERFLOW

        if self.header.tags_count+add_tags > self.header.tags_capacity:
            return ERR_RESIZE_REQUIRED

        if self.header.last_position + new_rec_size <= self.header.data_size:
            return 1
        else:
            return ERR_RESIZE_REQUIRED

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

        if (USHRT_MAX - self.header.data_size) < add_values_size:
            self.header.last_position = USHRT_MAX
            return NULL
        if (UCHAR_MAX - self.header.tags_capacity) < add_tags:
            self.header.last_position = USHRT_MAX
            return NULL
        if add_tags == 0 and add_values_size == 0:
            return NULL

        cdef uint16_t * magic_middle = _calc_offset_magic_middle(self)
        cdef uint16_t * magic_end = _calc_offset_magic_end(self)
        # Corrupted data integrity skip!
        if magic_middle[0] != FIX_MAGIC:
            return NULL
        if magic_end[0] != FIX_MAGIC:
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
            # We have open group, keep offset between group and self.values if main pointer will be changed
            open_group_offset = <void *> self.open_group - self.values
            open_group_tag = self.open_group.fix_rec.tag

        cdef int old_magic_middle_offset = (<void*>magic_middle) - (<void*>self)
        cdef int old_tags_length = (<void*>magic_end) - (<void*>magic_middle)

        # From `man 3 realloc`
        #    If realloc() fails, the original block is left untouched; it is not freed or moved
        cdef FIXMsgStruct * new_self = <FIXMsgStruct*>realloc(self, _calc_data_size(new_size, new_tags))
        if new_self == NULL:
            # MEMORY ERROR
            # On failure keep data untouched, but prevent any updates
            self.header.last_position = USHRT_MAX
            return NULL


        cdef uint16_t add_tags_offset = add_tags*sizeof(FIXOffsetMap)

        if new_size > 0:
            # To increase size capacity we need to move block of memory from magic middle + new_size bytes
            memmove(<void*> new_self + old_magic_middle_offset + add_values_size,
                    <void*> new_self + old_magic_middle_offset,
                    old_tags_length
                    )

        new_self.values = _calc_offset_values(new_self)
        new_self.header.data_size = <uint16_t> new_size
        new_self.header.n_reallocs += 1
        new_self.header.tags_capacity = new_tags

        # Tags must be updated after header changed
        new_self.tags = _calc_offset_tags(new_self)

        magic_middle = _calc_offset_magic_middle(new_self)
        magic_end = _calc_offset_magic_end(new_self)

        # Magic middle should be in place
        cyassert(magic_middle[0] == FIX_MAGIC)

        # # Make sure after tags capacity change without value size the magic_end is valid
        # cyassert(add_tags == 0 or magic_end[0] == FIX_MAGIC)

        # Set magic end because of data resize
        magic_end[0] = FIX_MAGIC

        if open_group_offset != -1:
            # Algo don't forget to align by new tags added
            new_self.open_group = <FIXGroupRec *> (new_self.values + open_group_offset)
            cyassert(new_self.open_group.fix_rec.tag == open_group_tag)
            cyassert(new_self.open_group.fix_rec.value_type == b'\x07')

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
        if self.header.is_read_only == 1:
            return ERR_DATA_READ_ONLY
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

        rc = FIXMsg.has_capacity(self, 1, value_size + sizeof(FIXRec))
        if rc <= 0:
            cyassert(rc != 0)  # 0 is returned when add_tag or value_size == 0
            if rc == ERR_DATA_OVERFLOW:
                # Critical error, not fixable, set error status
                self.header.last_position = USHRT_MAX
            return rc

        cdef FIXRec * rec

        cdef uint16_t _data_offset_idx =  FIXMsg._set_tag_offset(self, tag, last_position)

        if _data_offset_idx  >= USHRT_MAX-10:

            if _data_offset_idx == USHRT_MAX-2: #RESULT_DUPLICATE = 	65533 # USHRT_MAX-2
                # Duplicate or error
                return ERR_FIX_DUPLICATE_TAG
            elif _data_offset_idx == USHRT_MAX-3:  # TAG_NEED_RESIZE
                return ERR_RESIZE_REQUIRED
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
        cdef FIXRec * rec
        # Fill default return values
        value[0] = NULL
        value_size[0] = 0

        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED
        if value_type == b'\x07' or value_type == b'\x00':
            return ERR_FIX_NOT_ALLOWED
        if self.header.last_position == USHRT_MAX:
            return ERR_DATA_OVERFLOW

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

    @staticmethod
    cdef int group_start(FIXMsgStruct * self, uint16_t group_tag, uint16_t grp_n_elements, uint16_t n_tags, uint16_t *tags) nogil:
        """
        Initializes fix group

        Example:
            m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 200, 10)
            cdef uint16_t n_elements = 5
    
            cdef int i = 123
            cdef double f = 8907.889
            cdef char c = b'V'
            cdef char * s = b'my fancy string, it may be too long!'
    
            assert FIXMsg.group_start(m, 100, n_elements, 4,  [10, 11, 12, 13]) == 1
            for k in range(n_elements):
                # start_tag is mandatory! TAG ORDER MATTERS!
                FIXMsg.group_add_tag(m, 100, 10, &i, sizeof(int), b'i')
                FIXMsg.group_add_tag(m, 100, 11, &f, sizeof(double), b'f')
                # Other tags may be omitted or optional
                #FIXMsg.group_add_tag(m, 100, 12, &c, sizeof(char), b'c')
                FIXMsg.group_add_tag(m, 100, 13, s, strlen(s) + 1, b's')
            assert FIXMsg.group_finish(m, 100) == 1
    
            assert FIXMsg.group_count(m, 100) == 5
    
            FIXMsg.destroy(m)


        :param group_tag: unique group tag 
        :param grp_n_elements: number of elements in a grout
        :param n_tags: length of `tags` array, min 1, max 126
        :param tags: collection of child tags (must be unique, order matters!)
        :return: positive on success, negative on error
        """
        if self.header.is_read_only == 1:
            return ERR_DATA_READ_ONLY
        if self.open_group != NULL:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_FINISHED
        if grp_n_elements < 1:
            self.header.tag_errors += 1
            return ERR_GROUP_EMPTY
        if n_tags < 1:
            self.header.tag_errors += 1
            return ERR_GROUP_EMPTY
        if n_tags >= 127:
            self.header.tag_errors += 1
            return ERR_GROUP_TOO_MANY
        if group_tag == 0:
            self.header.tag_errors += 1
            return ERR_FIX_ZERO_TAG

        cyassert(tags != NULL)

        cdef int rc = 0
        cdef int base_len = sizeof(uint16_t) * n_tags + sizeof(uint16_t) * grp_n_elements

        cdef uint16_t last_position = self.header.last_position

        rc = FIXMsg.has_capacity(self, 1, base_len + sizeof(FIXGroupRec))
        if rc <= 0:
            if rc == ERR_DATA_OVERFLOW:
                # Invalidate message
                self.header.last_position = USHRT_MAX
            return rc

        cdef FIXGroupRec * rec
        if FIXMsg._set_tag_offset(self, group_tag, last_position) >= USHRT_MAX - 10:
            self.header.tag_errors += 1
            return ERR_FIX_DUPLICATE_TAG

        # New value added successfully
        rec = <FIXGroupRec *> (self.values + last_position)

        # Group data alignment
        # struct FIXGroupRec
        #     FIXRec fix_rec
        #         uint16_t tag
        #         char value_type
        #         uint16_t value_len
        #     uint16_t grp_n_elements
        #     uint16_t n_tags
        #     uint16_t * group_tags_values         # List of all tags in a group (must be n_tags length)
        #         uint16_t *tag1 (is mandatory!)
        #         ...
        #         uint16_t *tag_n (len n_tags)
        #     uint16_t * group_elements_offsets     # Used for fast group search by index
        #         uint16_t *el_1 offset
        #         ...
        #         uint16_t *el_n offset (len grp_n_elements)

        rec.fix_rec.tag = group_tag
        rec.fix_rec.value_type = b'\x07'  # Special type reserved for groups
        # This is a base length extra to sizeof(FIXRec)
        rec.fix_rec.value_len = base_len
        rec.grp_n_elements = grp_n_elements
        rec.n_tags = n_tags
        rec.current_element = 0  # First iteration will make it 0
        rec.current_tag_len = -1
        self.open_group = rec

        cdef uint16_t i, j
        cdef uint16_t *fix_data_tags = <uint16_t *> (self.values + last_position + sizeof(FIXGroupRec))

        for i in range(n_tags):
            cyassert(n_tags < 127)
            if tags[i] == 0:
                self.header.tag_errors += 1
                return ERR_FIX_ZERO_TAG
            # Quick check for duplicates
            if tags[i] == group_tag:
                self.header.tag_errors += 1
                return ERR_GROUP_DUPLICATE_TAG

            for j in range(n_tags):
                if i != j and tags[i] == tags[j]:
                    self.header.tag_errors += 1
                    return ERR_GROUP_DUPLICATE_TAG

            if FIXMsg._get_tag_offset(self, tags[i]) != USHRT_MAX - 1:  # DEF RESULT_NOT_FOUND = 	65534 # USHRT_MAX-1
                # Group tags must be unique across global tags!
                self.header.tag_errors += 1
                return ERR_GROUP_DUPLICATE_TAG

            fix_data_tags[i] = tags[i]

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (self.values + last_position + sizeof(FIXGroupRec) + n_tags * sizeof(uint16_t))

        for i in range(grp_n_elements):
            fix_data_el_offsets[i] = USHRT_MAX

        self.header.last_position += sizeof(FIXGroupRec) + n_tags * sizeof(uint16_t) + grp_n_elements * sizeof(uint16_t)
        return 1
    
    @staticmethod
    cdef int group_add_tag(FIXMsgStruct * self, uint16_t group_tag, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil:
        """
        Adds new child tag into a group

        Notes:
            - This function requires previous group_start() call
            - `tag` must present in group_start(..., *tags) call
            - always add `tag[0]` (start tag) in group_start(..., *tags[0]) call
            - child tags must be unique across FIX messages (but allowed duplication within the same group_tag)
            - you must add the count of start_tags == value of group_start(..., `grp_n_elements`,...)
            - tag order matters and must be the same as in FIX specification and group_start(..., *tags) order

        :param group_tag: parent group tag 
        :param tag: child group tag
        :param value: child tag value
        :param value_size: child tag size
        :param value_type: child tag type
        :return: positive on success, zero or negative on error 
        """
        if self.header.is_read_only == 1:
            return ERR_DATA_READ_ONLY
        if self.open_group == NULL:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_STARTED
        if group_tag == 0:
            self.header.tag_errors += 1
            return ERR_FIX_ZERO_TAG
        if tag == 0:
            self.header.tag_errors += 1
            return ERR_FIX_ZERO_TAG
        if value_type == b'\x07' or value_type == b'\x00':
            self.header.tag_errors += 1
            return ERR_FIX_NOT_ALLOWED
        if value_size == 0:
            self.header.tag_errors += 1
            return ERR_UNEXPECTED_TYPE_SIZE
        if value_size > 1024:
            self.header.tag_errors += 1
            return ERR_FIX_VALUE_TOOLONG

        if group_tag != self.open_group.fix_rec.tag:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_MATCH

        cdef int rc = FIXMsg.has_capacity(self, 0, value_size + sizeof(FIXRec))
        if rc <= 0:
            if rc == ERR_DATA_OVERFLOW:
                self.header.last_position = USHRT_MAX
            return rc

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> self.open_group + sizeof(FIXGroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> self.open_group + sizeof(FIXGroupRec) + self.open_group.n_tags * sizeof(uint16_t))
        cdef uint16_t last_position = self.header.last_position
        cdef int tag_count = 0
        cdef int k = 0
        cdef FIXRec * rec
        cdef int tag_offset = 0

        if self.open_group.current_element == 0 and self.open_group.current_tag_len == -1:
            # Just initializing
            if tag != fix_data_tags[0]:
                self.header.tag_errors += 1
                return ERR_GROUP_START_TAG_EXPECTED

        if tag == fix_data_tags[0]:
            # Starting tag, add next element
            if self.open_group.current_tag_len != -1:
                self.open_group.current_element += 1

            self.open_group.current_tag_len = 0

            if self.open_group.current_element >= <int> self.open_group.grp_n_elements:
                self.header.tag_errors += 1
                return ERR_GROUP_EL_OVERFLOW

            fix_data_el_offsets[self.open_group.current_element] = last_position
        else:
            for i in range(self.open_group.n_tags):
                if tag == fix_data_tags[i]:
                    tag_count += 1
                    tag_offset = 0
                    k = 0
                    while k < self.open_group.current_tag_len:
                        rec = <FIXRec *> (self.values + fix_data_el_offsets[self.open_group.current_element] + tag_offset)
                        for j in range(i, self.open_group.n_tags):
                            if rec.tag == fix_data_tags[j]:
                                if i == j:
                                    self.header.tag_errors += 1
                                    return ERR_GROUP_DUPLICATE_TAG
                                else:
                                    self.header.tag_errors += 1
                                    return ERR_GROUP_TAG_WRONG_ORDER

                        tag_offset += sizeof(FIXRec) + rec.value_len
                        k += 1
                    break
            if tag_count == 0:
                self.header.tag_errors += 1
                return ERR_GROUP_TAG_NOT_INGROUP

        rec = <FIXRec *> (self.values + last_position)
        rec.tag = tag
        rec.value_type = value_type
        rec.value_len = value_size

        self.header.last_position += sizeof(FIXRec) + value_size
        self.open_group.fix_rec.value_len += sizeof(FIXRec) + value_size
        self.open_group.current_tag_len += 1

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
    cdef int group_get(FIXMsgStruct * self, uint16_t group_tag, uint16_t el_idx, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil:
        """
        Get group.element.tag value

        :param group_tag: finished group tag
        :param el_idx:  element index (must be >= 0 and < grp_n_elements)
        :param tag: child tag in the group
        :param value: pointer to void* buffer of the tag value
        :param value_size:  tag value size
        :param value_type: tag value type (must match to group_add_tag(..., `value_type`))        
        :return: 1 if found, 0 if not found, negative on error
        """

        # Reset pointers before handling any errors
        value[0] = NULL
        value_size[0] = 0
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if group_tag == 35:
            return ERR_FIX_NOT_ALLOWED

        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED

        if self.header.last_position == USHRT_MAX:
            return ERR_DATA_OVERFLOW

        cdef FIXGroupRec * rec

        cdef uint16_t data_offset = FIXMsg._get_tag_offset(self, group_tag)
        if data_offset >= USHRT_MAX - 10:
            if data_offset == USHRT_MAX - 1:
                # Tag not found
                return ERR_NOT_FOUND
            if data_offset == USHRT_MAX - 2:
                # Possibly duplicate tag
                return ERR_FIX_DUPLICATE_TAG
            # Likely tag number is too high, other generic error, we must stop anyway
            return ERR_DATA_OVERFLOW

        # New value added successfully
        rec = <FIXGroupRec *> (self.values + data_offset)

        if rec.fix_rec.value_type != b'\x07':
            # Type mismatch
            self.header.tag_errors += 1
            return ERR_GROUP_CORRUPTED

        cyassert(rec.n_tags > 0)
        cyassert(rec.grp_n_elements > 0)

        if el_idx >= rec.grp_n_elements:
            return ERR_GROUP_EL_OVERFLOW

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> rec + sizeof(FIXGroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> rec + sizeof(FIXGroupRec) + rec.n_tags * sizeof(uint16_t))

        cdef bint tag_in_group = False
        for i in range(rec.n_tags):
            if tag == fix_data_tags[i]:
                tag_in_group = True
                break
        if not tag_in_group:
            return ERR_GROUP_TAG_NOT_INGROUP

        cdef uint16_t el_offset = fix_data_el_offsets[el_idx]
        cdef FIXRec * trec = <FIXRec *> (self.values + el_offset)
        cdef uint16_t tag_offset = 0

        cyassert(el_offset != USHRT_MAX)
        cyassert(trec != NULL)

        while True:
            if el_offset + tag_offset > self.header.data_size:
                self.header.tag_errors += 1
                return ERR_GROUP_CORRUPTED

            trec = <FIXRec *> (self.values + el_offset + tag_offset)

            if trec.tag == 0:
                return ERR_NOT_FOUND

            if tag_offset != 0:
                if trec.tag == fix_data_tags[0]:
                    # Next start tag
                    return ERR_NOT_FOUND
            else:
                if trec.tag != fix_data_tags[0]:
                    self.header.tag_errors += 1
                    # Expected to be a start tag
                    return ERR_GROUP_CORRUPTED

            if trec.tag == tag:
                break
            else:
                tag_offset += (trec.value_len + sizeof(FIXRec))

        if trec.value_type != value_type:
            return ERR_FIX_TYPE_MISMATCH

        value_size[0] = trec.value_len
        value[0] = (<void *> trec + sizeof(FIXRec))
        return 1
    
    @staticmethod
    cdef int group_finish(FIXMsgStruct * self, uint16_t group_tag) nogil:
        """
        Finish tag group, must be called after 

        :param group_tag: 
        :return: 
        """
        if self.header.is_read_only == 1:
            return ERR_DATA_READ_ONLY
        if self.open_group == NULL:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_STARTED
        if group_tag != self.open_group.fix_rec.tag:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_MATCH
        if self.open_group.current_element != self.open_group.grp_n_elements - 1:
            self.header.tag_errors += 1
            return ERR_GROUP_NOT_COMPLETED
        cdef uint16_t last_position = self.header.last_position

        cdef int rc = FIXMsg.has_capacity(self, 0, sizeof(FIXRec))
        if rc <= 0:
            if rc == ERR_DATA_OVERFLOW:
                self.header.last_position = USHRT_MAX
            self.header.tag_errors += 1
            return rc

        rec = <FIXRec *> (self.values + last_position)

        rec.tag = 0
        rec.value_type = b'\0'
        rec.value_len = 0

        self.header.last_position += sizeof(FIXRec)
        self.open_group.fix_rec.value_len += sizeof(FIXRec)
        self.open_group = NULL

        return 1
    
    @staticmethod
    cdef int group_count(FIXMsgStruct * self, uint16_t group_tag) nogil:
        """
        Gets number of element of `group_tag`, or zero if not found

        :param group_tag: some valid group tag
        :return: positive count if group exists, 0 if not exists, negative on error
        """
        if self.header.last_position == USHRT_MAX:
            return ERR_DATA_OVERFLOW
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG
        if group_tag == 35:
            return ERR_FIX_NOT_ALLOWED
        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED

        cdef uint16_t data_offset = FIXMsg._get_tag_offset(self, group_tag)
        if data_offset >= USHRT_MAX - 10:
            if data_offset == USHRT_MAX - 1:
                # Tag not found
                return ERR_NOT_FOUND
            if data_offset == USHRT_MAX - 2:
                # Possibly duplicate tag
                return ERR_FIX_DUPLICATE_TAG
            # Likely tag number is too high, other generic error, we must stop anyway
            return ERR_DATA_OVERFLOW

        # New value added successfully
        cdef FIXGroupRec * rec = <FIXGroupRec *> (self.values + data_offset)

        if rec.fix_rec.value_type != b'\x07':
            self.header.tag_errors += 1
            return ERR_GROUP_CORRUPTED

        return rec.grp_n_elements

    #
    #  Primitive type getters / setters
    #
    #
    @staticmethod
    cdef int set_int(FIXMsgStruct * self, uint16_t tag, int value) nogil:
        """
        Set signed integer tag

        :param tag: any valid tag
        :param value: any integer value
        :return: positive on success, negative on error
        """
        return FIXMsg.set(self, tag, &value, sizeof(int), b'i')

    @staticmethod
    cdef int * get_int(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Get signed integer tag

        :param tag: any valid tag
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b'i')
        if rc > 0 and size == sizeof(int):
            return <int *> value
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL

    @staticmethod
    cdef int set_bool(FIXMsgStruct * self, uint16_t tag, bint value) nogil:
        """
        Set boolean

        :param tag: any valid tag
        :param value: must be 0 or 1
        :return: positive on success, negative on error
        """
        if value != 0 and value != 1:
            self.header.tag_errors += 1
            return ERR_UNEXPECTED_TYPE_SIZE
        cdef char v = <char> value
        return FIXMsg.set(self, tag, &v, sizeof(char), b'b')

    @staticmethod
    cdef int8_t * get_bool(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Get boolean

        :param tag:
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b'b')
        if rc > 0 and size == sizeof(char):
            if (<int8_t*>value)[0] == 1 or (<int8_t*>value)[0] == 0:
                return <int8_t *> value
            else:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
                return NULL
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL

    @staticmethod
    cdef int set_char(FIXMsgStruct * self, uint16_t tag, char value) nogil:
        """
        Set char

        :param tag: any valid tag
        :param value: must be > 20 and < 127 (i.e. all printable chars are allowed)
        :return: positive on success, negative on error
        """
        if value < 20 or value == 127:
            # All negative and control ASCII char are not allowed
            self.header.tag_errors += 1
            return ERR_UNEXPECTED_TYPE_SIZE

        return FIXMsg.set(self, tag, &value, sizeof(char), b'c')

    @staticmethod
    cdef char * get_char(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Get char

        :param tag:
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b'c')
        if rc > 0 and size == sizeof(char):
            return <char *> value
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL

    @staticmethod
    cdef int set_double(FIXMsgStruct * self, uint16_t tag, double value) nogil:
        """
        Set floating point number (type double)

        :param tag: any valid tag
        :param value: any double value
        :return: positive on success, negative on error
        """
        return FIXMsg.set(self, tag, &value, sizeof(double), b'f')

    @staticmethod
    cdef double * get_double(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Get double tag

        :param tag:
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b'f')
        if rc > 0 and size == sizeof(double):
            return <double *> value
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL

    @staticmethod
    cdef int set_utc_timestamp(FIXMsgStruct * self, uint16_t tag, long value_ns) nogil:
        """
        Set UTC timestamp as nanoseconds since epoch (long)

        :param tag: any valid tag
        :param value_ns: nanoseconds since epoch
        :return: positive on success, negative on error
        """
        return FIXMsg.set(self, tag, &value_ns, sizeof(long), b't')

    @staticmethod
    cdef long * get_utc_timestamp(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Gets UTC timestamp as nanoseconds since epoch

        :param tag:
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b't')
        if rc > 0 and size == sizeof(long):
            return <long *> value
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL

    @staticmethod
    cdef int set_str(FIXMsgStruct * self, uint16_t tag, char *value, uint16_t length) nogil:
        """
        Set string field

        :param tag: any valid tag
        :param value: any string of length > 0 and < 1024
        :param length: string length (without \0 char, i.e. length of 'abc' == 3),
                       if length = 0, the function will use strlen(value)
                       if length is known you should pass is for performance reasons
        :return: positive on success, negative on error
        """
        if length == 0:
            length = strlen(value)
        if length == 0:
            self.header.tag_errors += 1
            return ERR_UNEXPECTED_TYPE_SIZE
        return FIXMsg.set(self, tag, value, length + 1, b's')

    @staticmethod
    cdef char * get_str(FIXMsgStruct * self, uint16_t tag) nogil:
        """
        Get string field

        :param tag: any valid tag
        :return: pointer to value, or NULL on error + sets last_error
        """
        self.header.last_error = 1
        cdef void * value
        cdef uint16_t size
        cdef char * result
        cdef int rc = FIXMsg.get(self, tag, &value, &size, b's')
        result = <char *> value
        if rc > 0 and size > 1 and result[0] != b'\0':
            return result
        else:
            if rc > 0:
                self.header.last_error = ERR_UNEXPECTED_TYPE_SIZE
            else:
                self.header.last_error = rc
            return NULL