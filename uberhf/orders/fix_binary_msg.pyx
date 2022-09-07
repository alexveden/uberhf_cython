from libc.stdlib cimport calloc, realloc, free
from libc.string cimport memcpy, memset, strlen
from libc.limits cimport USHRT_MAX
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from uberhf.orders.fix_tag_tree cimport binary_tree_destroy, binary_tree_create, binary_tree_set_offset, binary_tree_get_offset
DEF FIX_MAGIC = 22093



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



cdef class FIXBinaryMsg:
    def __cinit__(self, char msg_type, uint16_t data_size, uint16_t tag_tree_capacity=64):
        cdef uint16_t _data_size = max(data_size, 128)
        # Use calloc to set memory to zero!
        self._data = calloc(sizeof(FIXBinaryHeader) + _data_size, 1)
        if self._data == NULL:
            raise MemoryError()
        self.values = self._data + sizeof(FIXBinaryHeader)

        self.tag_tree = binary_tree_create(tag_tree_capacity)

        self.open_group = NULL

        self.header = <FIXBinaryHeader*>self._data
        self.header.magic_number = FIX_MAGIC
        self.header.msg_type = msg_type
        self.header.data_size = _data_size
        self.header.last_position = 0
        self.header.n_reallocs = 0
        self.header.tag_duplicates = 0

    def __dealloc__(self):
        if self._data != NULL:
            free(self._data)
            self._data = NULL

        if self.tag_tree != NULL:
            binary_tree_destroy(self.tag_tree)
            self.tag_tree = NULL

    cdef bint is_valid(self) nogil:
        return self.header.magic_number == FIX_MAGIC and \
               self.header.last_position < USHRT_MAX and \
               self.header.tag_duplicates == 0 and \
               self.header.msg_type > 0

    cdef int _request_new_space(self, size_t extra_size) nogil:
        """
        Grow binary FIX data container
        
        :param extra_size: size on data in bytes
        :return: negative on error, positive on success
        """
        cyassert(self.header.last_position <= USHRT_MAX)

        if <uint16_t>(USHRT_MAX - self.header.last_position) <= extra_size:
            self.header.last_position = USHRT_MAX
            return ERR_DATA_OVERFLOW

        cdef size_t data_size = self.header.data_size

        if self.header.last_position + extra_size <= data_size:
            return 1

        # Resize to extra_size + approx 10 double tags room
        cdef size_t new_size = min(USHRT_MAX, data_size + extra_size + (sizeof(FIXRec)+sizeof(double))*10)

        if new_size == USHRT_MAX and data_size == USHRT_MAX:
            # Allow first max capacity resize request to pas
            self.header.last_position = USHRT_MAX
            return ERR_DATA_OVERFLOW

        # Only new_size increase allowed!
        cyassert(new_size > data_size)
        cyassert(new_size <= USHRT_MAX)

        cdef int open_group_offset = -1
        cdef int open_group_tag = -1
        if self.open_group != NULL:
            # We have open group, keep offset between group and self.values if main pointer will be changed
            open_group_offset = <void*>self.open_group - self.values
            open_group_tag = self.open_group.fix_rec.tag

        self._data = realloc(self._data, new_size + sizeof(FIXBinaryHeader))
        cyassert(self._data != NULL)
        if self._data == NULL:
            # Memory error!
            self.header = NULL
            self.values = NULL
            return ERR_MEMORY_ERROR

        # Zero memory as well, because realloc doesn't do this by default
        memset(self._data + data_size + sizeof(FIXBinaryHeader), 0, new_size-data_size)

        # We must reset pointers to values/header because self._data may be changed
        self.values = self._data + sizeof(FIXBinaryHeader)
        self.header = <FIXBinaryHeader *> self._data
        self.header.data_size = <uint16_t>new_size
        self.header.n_reallocs += 1

        if self.open_group != NULL:
            self.open_group = <GroupRec *>(self.values + open_group_offset)
            cyassert(self.open_group.fix_rec.tag == open_group_tag)
            cyassert(self.open_group.fix_rec.value_type == b'\x07')

        cyassert(self.header.magic_number == FIX_MAGIC)
        return 1

    cdef int set(self, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil:
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
        cyassert(self.header != NULL)
        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED

        if value_type == b'\x07' or value_type == b'\x00':
            return ERR_FIX_NOT_ALLOWED

        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 35:
            # MsgType must be set at constructor
            return ERR_FIX_NOT_ALLOWED
        if value_size > 1024:
            return ERR_FIX_VALUE_TOOLONG

        cdef int rc = 0
        cdef uint16_t last_position = self.header.last_position

        if self._request_new_space(value_size + sizeof(FIXRec)) < 0:
            return ERR_DATA_OVERFLOW

        cdef FIXRec * rec

        cdef uint16_t _data_offset_idx = binary_tree_set_offset(self.tag_tree, tag, last_position)

        if _data_offset_idx  >= USHRT_MAX-10:
            if _data_offset_idx == USHRT_MAX-2: #RESULT_DUPLICATE = 	65533 # USHRT_MAX-2
                # Duplicate or error
                self.header.tag_duplicates += 1
                return ERR_FIX_DUPLICATE_TAG
            else:
                # Other generic error, typically oveflow
                return ERR_DATA_OVERFLOW



        # New value added successfully
        rec = <FIXRec*>(self.values + last_position)

        # Assuming that the new memory should be set to zero
        cyassert(rec.tag == 0)
        cyassert(rec.value_type == b'\0')
        cyassert(rec.value_len == 0)

        rec.tag = tag
        rec.value_type = value_type
        rec.value_len = value_size

        if value_type == b's':
            cyassert(strlen(<char*>value) + 1 == value_size)   # strlen mismatch, must include \0, and check if value != &((*char)some_string)
            cyassert((<char*>value)[value_size-1] == b'\0')    # String must be null-terminated!

        memcpy(self.values + last_position + sizeof(FIXRec), value, value_size)
        self.header.last_position += sizeof(FIXRec) + value_size
        return 1

    cdef int get(self, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil:
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
        cyassert(self.header != NULL)
        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED
        if value_type == b'\x07' or value_type == b'\x00':
            return ERR_FIX_NOT_ALLOWED

        cdef FIXRec * rec
        # Fill default return values
        value[0] = NULL
        value_size[0] = 0
        # TODO: implement value_type = 0, for skipping type checks

        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 35:
            # Getting message type
            if value_type != b'c':
                return ERR_FIX_TYPE_MISMATCH
            value[0] = &self.header.msg_type
            value_size[0] = sizeof(char)
            return 1

        cdef uint16_t data_offset = binary_tree_get_offset(self.tag_tree, tag)
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

    cdef int group_start(self, uint16_t group_tag, uint16_t grp_n_elements, uint16_t n_tags, uint16_t *tags) nogil:
        """
        
        :param group_tag: 
        :param grp_n_elements: 
        :param n_tags: 
        :param tags: 
        :return: 
        """

        """
        Group data alignment
        struct GroupRec
            FIXRec fix_rec
                uint16_t tag
                char value_type
                uint16_t value_len
            uint16_t grp_n_elements
            uint16_t n_tags
            uint16_t * group_tags_values         # List of all tags in a group (must be n_tags length)
                uint16_t *tag1 (is mandatory!)
                ...
                uint16_t *tag_n (len n_tags)
            uint16_t * group_elements_offsets     # Used for fast group search by index
                uint16_t *el_1 offset
                ...
                uint16_t *el_n offset (len grp_n_elements)
        """
        cyassert(self.header != NULL)
        if self.open_group != NULL:
            return ERR_GROUP_NOT_FINISHED
        if grp_n_elements < 1:
            return ERR_GROUP_EMPTY
        if n_tags < 1:
            return ERR_GROUP_EMPTY
        if n_tags >= 127:
            return ERR_GROUP_TOO_MANY
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG

        cyassert(tags != NULL)

        cdef int rc = 0
        cdef int base_len = sizeof(uint16_t) * n_tags + sizeof(uint16_t) * grp_n_elements

        cdef uint16_t last_position = self.header.last_position

        if self._request_new_space(base_len + sizeof(GroupRec)) < 0:
            return ERR_DATA_OVERFLOW

        cdef GroupRec * rec
        if binary_tree_set_offset(self.tag_tree, group_tag, last_position) >= USHRT_MAX-2:
            self.header.tag_duplicates += 1
            return ERR_FIX_DUPLICATE_TAG

        # New value added successfully
        rec = <GroupRec *> (self.values + last_position)

        # Check if the new memory block is zeroed
        cyassert(rec.fix_rec.tag == 0)
        cyassert(rec.fix_rec.value_type == b'\0')
        cyassert(rec.fix_rec.value_len == 0)

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
        cdef uint16_t *fix_data_tags = <uint16_t *>(self.values + last_position + sizeof(GroupRec))


        for i in range(n_tags):
            cyassert(n_tags < 127)
            if tags[i] == 0:
                return ERR_FIX_ZERO_TAG
            # Quick check for duplicates
            if tags[i] == group_tag:
                return ERR_GROUP_DUPLICATE_TAG

            for j in range(n_tags):
                if i != j and tags[i] == tags[j]:
                    return ERR_GROUP_DUPLICATE_TAG

            if binary_tree_get_offset(self.tag_tree, tags[i]) != USHRT_MAX-1:  # DEF RESULT_NOT_FOUND = 	65534 # USHRT_MAX-1
                # Group tags must be unique across global tags!
                return ERR_GROUP_DUPLICATE_TAG

            fix_data_tags[i] = tags[i]

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (self.values + last_position + sizeof(GroupRec) + n_tags*sizeof(uint16_t))

        for i in range(grp_n_elements):
            fix_data_el_offsets[i] = USHRT_MAX

        self.header.last_position += sizeof(GroupRec) + n_tags*sizeof(uint16_t) + grp_n_elements*sizeof(uint16_t)
        return 1

    cdef int group_add_tag(self, uint16_t group_tag, uint16_t tag, void * value, uint16_t value_size, char value_type) nogil:
        cyassert(self.header != NULL)
        if self.open_group == NULL:
            return ERR_GROUP_NOT_STARTED
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 0:
            return ERR_FIX_ZERO_TAG
        if value_type == b'\x07' or value_type == b'\x00':
            return ERR_FIX_NOT_ALLOWED

        if group_tag != self.open_group.fix_rec.tag:
            return ERR_GROUP_NOT_MATCH

        if self._request_new_space(value_size + sizeof(FIXRec)) < 0:
            return ERR_DATA_OVERFLOW

        cdef uint16_t *fix_data_tags = <uint16_t *>(<void*>self.open_group + sizeof(GroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void*>self.open_group + sizeof(GroupRec) + self.open_group.n_tags*sizeof(uint16_t))
        cdef uint16_t last_position = self.header.last_position
        cdef int tag_count = 0
        cdef int k = 0
        cdef FIXRec * rec
        cdef int tag_offset = 0


        if self.open_group.current_element == 0 and self.open_group.current_tag_len == -1:
            # Just initializing
            if tag != fix_data_tags[0]:
                return ERR_GROUP_START_TAG_EXPECTED

        if tag == fix_data_tags[0]:
            # Starting tag, add next element
            if self.open_group.current_tag_len != -1:
                self.open_group.current_element += 1

            self.open_group.current_tag_len = 0

            if self.open_group.current_element >= <int> self.open_group.grp_n_elements:
                return ERR_GROUP_EL_OVERFLOW

            fix_data_el_offsets[self.open_group.current_element] = last_position
        else:
            for i in range(self.open_group.n_tags):
                if tag == fix_data_tags[i]:
                    tag_count += 1
                    tag_offset = 0
                    k = 0
                    while k < self.open_group.current_tag_len:
                        rec = <FIXRec*>(self.values + fix_data_el_offsets[self.open_group.current_element] + tag_offset)
                        for j in range(i, self.open_group.n_tags):
                            if rec.tag == fix_data_tags[j]:
                                if i == j:
                                    return ERR_GROUP_DUPLICATE_TAG
                                else:
                                    return ERR_GROUP_TAG_WRONG_ORDER

                        tag_offset += sizeof(FIXRec) + rec.value_len
                        k += 1
                    break
            if tag_count == 0:
                return ERR_GROUP_TAG_NOT_INGROUP

        rec = <FIXRec *> (self.values + last_position)

        # Check if the new memory block is zeroed
        cyassert(rec.tag == 0)
        cyassert(rec.value_type == b'\0')
        cyassert(rec.value_len == 0)

        rec.tag = tag
        rec.value_type = value_type
        rec.value_len = value_size

        #cybreakpoint(group_tag == 10 and tag == 17)
        if value_type == b's':
            cyassert(strlen(<char*>value) + 1 == value_size)   # strlen mismatch, must include \0, and check if value != &((*char)some_string)
            cyassert((<char*>value)[value_size-1] == b'\0')    # String must be null-terminated!

        memcpy(self.values + last_position + sizeof(FIXRec), value, value_size)
        self.header.last_position += sizeof(FIXRec) + value_size
        self.open_group.fix_rec.value_len += sizeof(FIXRec) + value_size
        self.open_group.current_tag_len += 1
        return 1

    cdef int group_get(self, uint16_t group_tag, uint16_t el_idx, uint16_t tag, void ** value, uint16_t * value_size, char value_type) nogil:
        cyassert(self.header != NULL)
        # Reset pointers before handling any errors
        value[0] = NULL
        value_size[0] = 0
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG
        if tag == 0:
            return ERR_FIX_ZERO_TAG

        if self.open_group != NULL:
           return ERR_GROUP_NOT_FINISHED

        cdef GroupRec * rec

        cdef uint16_t data_offset = binary_tree_get_offset(self.tag_tree, group_tag)
        if data_offset >= USHRT_MAX-10:
            if data_offset == USHRT_MAX-1:
                # Tag not found
                return ERR_NOT_FOUND
            if data_offset == USHRT_MAX-2:
                # Possibly duplicate tag
                return ERR_FIX_DUPLICATE_TAG
            # Likely tag number is too high, other generic error, we must stop anyway
            return ERR_DATA_OVERFLOW

        # New value added successfully
        rec = <GroupRec *> (self.values + data_offset)

        if rec.fix_rec.value_type != b'\x07':
            # Type mismatch
            return ERR_GROUP_CORRUPTED

        cyassert(rec.n_tags > 0)
        cyassert(rec.grp_n_elements > 0)

        if el_idx >= rec.grp_n_elements:
            return ERR_GROUP_EL_OVERFLOW

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> rec + sizeof(GroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> rec + sizeof(GroupRec) + rec.n_tags * sizeof(uint16_t))

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
                    # Expected to be a start tag
                    return ERR_GROUP_CORRUPTED

            if trec.tag == tag:
                break
            else:
                tag_offset += (trec.value_len + sizeof(FIXRec))

        if trec.value_type != value_type:
            return ERR_FIX_TYPE_MISMATCH

        value_size[0] = trec.value_len
        value[0] = (<void*>trec + sizeof(FIXRec))
        return 1

    cdef int group_finish(self, uint16_t group_tag) nogil:
        cyassert(self.header != NULL)
        if self.open_group == NULL:
            return ERR_GROUP_NOT_STARTED
        if group_tag != self.open_group.fix_rec.tag:
            return ERR_GROUP_NOT_MATCH
        if self.open_group.fix_rec.value_len == 0:
            return ERR_GROUP_NOT_COMPLETED
        if self.open_group.current_element != self.open_group.grp_n_elements-1:
            return ERR_GROUP_NOT_COMPLETED
        cdef uint16_t last_position = self.header.last_position

        if self._request_new_space(sizeof(FIXRec)) < 0:
            return ERR_DATA_OVERFLOW

        rec = <FIXRec *> (self.values + last_position)

        rec.tag = 0
        rec.value_type = b'\0'
        rec.value_len = 0

        self.header.last_position += sizeof(FIXRec)
        self.open_group.fix_rec.value_len += sizeof(FIXRec)
        self.open_group = NULL

        return 1

    cdef int group_count(self, uint16_t group_tag) nogil:
        cyassert(self.header != NULL)
        if self.header.last_position == USHRT_MAX:
            return ERR_DATA_OVERFLOW
        if group_tag == 0:
            return ERR_FIX_ZERO_TAG
        if self.open_group != NULL:
           return ERR_GROUP_NOT_FINISHED

        cdef uint16_t data_offset = binary_tree_get_offset(self.tag_tree, group_tag)
        if data_offset >= USHRT_MAX-10:
            if data_offset == USHRT_MAX-1:
                # Tag not found
                return ERR_NOT_FOUND
            if data_offset == USHRT_MAX-2:
                # Possibly duplicate tag
                return ERR_FIX_DUPLICATE_TAG
            # Likely tag number is too high, other generic error, we must stop anyway
            return ERR_DATA_OVERFLOW

        # New value added successfully
        cdef GroupRec * rec = <GroupRec *> (self.values + data_offset)

        if rec.fix_rec.value_type != b'\x07':
            return ERR_GROUP_CORRUPTED

        return rec.grp_n_elements
