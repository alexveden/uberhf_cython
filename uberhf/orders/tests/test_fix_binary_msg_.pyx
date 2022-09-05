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
from uberhf.includes.asserts cimport cybreakpoint
import os
import pytest
from libc.limits cimport USHRT_MAX

from uberhf.orders.fix_binary_msg cimport *

class CyBinaryMsgTestCase(unittest.TestCase):
    def test_fix_tag_hashmap(self):
        h = FIXTagHashMap()
        cdef FIXOffsetMap offset
        cdef FIXOffsetMap * p_offset

        assert h.item_size == sizeof(FIXOffsetMap)

        offset.tag = 1
        offset.data_offset = 123
        assert h.set(&offset) == NULL

        offset.tag = 10
        offset.data_offset = 222
        assert h.set(&offset) == NULL

        assert h.count() == 2

        offset.tag = 1
        offset.data_offset = 333
        assert h.set(&offset) != NULL

        p_offset = <FIXOffsetMap *>h.get(&offset)
        assert p_offset != NULL
        assert p_offset.tag == 1
        assert p_offset.data_offset == 333

    def test_init_msg(self):
        cdef FIXBinaryMsg m = FIXBinaryMsg(<char>b'C', 0)

        assert m.header.data_size == 128
        assert m.header.msg_type == b'C'
        assert m.header.magic_number == 22093
        assert m.header.last_position == 0
        assert m.header.n_reallocs == 0
        assert m.header.tag_duplicates == 0
        assert m.open_group == NULL

        m = FIXBinaryMsg(<char> b'C', 1000)

        assert m.header.data_size == 1000
        assert m.tag_hashmap.count() == 0
        assert m.tag_hashmap.item_size == sizeof(FIXOffsetMap)

    def test_get_set_raw(self):
        cdef FIXBinaryMsg m = FIXBinaryMsg(<char>b'C', 0)
        cdef int value = 123

        assert m.set(11, &value, sizeof(int), b'i') == 1
        assert m.tag_hashmap.count() == 1

        cdef void * p_value = NULL
        cdef uint16_t p_size = 0

        self.assertEqual(m.get(11, &p_value, &p_size, b'i'), 1)
        self.assertEqual((<int*>p_value)[0], 123)
        self.assertEqual(p_size, sizeof(int))

        # Not found
        self.assertEqual(m.get(1, &p_value, &p_size, b'i'), 0)
        assert p_value == NULL
        assert p_size == 0

        # Type mismatch
        self.assertEqual(m.get(11, &p_value, &p_size, b'c'), -2)
        assert p_value == NULL
        assert p_size == 0

    def test_get_set_duplicate(self):
        cdef FIXBinaryMsg m = FIXBinaryMsg(<char>b'C', 0)
        cdef int value = 123
        assert m.set(11, &value, sizeof(int), b'i') == 1
        assert m.set(11, &value, sizeof(int), b'i') == -1
        assert m.tag_hashmap.count() == 1
        assert m.header.tag_duplicates == 1

        cdef void * p_value = NULL
        cdef uint16_t p_size = 0

        # Get also return errors for duplicated tags
        self.assertEqual(m.get(11, &p_value, &p_size, b'i'), -1)
        assert p_value == NULL
        assert p_size == 0


    def test_get_set_multiple_base_types(self):
        cdef FIXBinaryMsg m = FIXBinaryMsg(<char>b'C', 200)
        cdef int i = 123
        cdef double f = 8907.889
        cdef char c = b'V'
        cdef char * s = b'my fancy string'
        cdef FIXRec* rec

        cdef int prev_pos = m.header.last_position
        assert m.set(1, &i, sizeof(int), b'i') == 1

        rec = <FIXRec*>(m.values + prev_pos)
        assert rec.tag == 1
        assert rec.value_type == b'i'
        assert rec.value_len == sizeof(int)

        prev_pos = sizeof(FIXRec) + sizeof(int)
        assert m.header.last_position == prev_pos
        assert m.set(2, &f, sizeof(double), b'f') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 2
        assert rec.value_type == b'f'
        assert rec.value_len == sizeof(double)

        prev_pos += sizeof(FIXRec) + sizeof(double)
        assert m.header.last_position == prev_pos

        assert m.set(3, &c, sizeof(char), b'c') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 3
        assert rec.value_type == b'c'
        assert rec.value_len == sizeof(char)


        prev_pos += sizeof(FIXRec) + sizeof(char)
        assert m.header.last_position == prev_pos
        assert m.set(4, s, strlen(s)+1, b's') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 4
        assert rec.value_type == b's'
        assert rec.value_len == strlen(s)+1

        prev_pos += sizeof(FIXRec) + strlen(s)+1
        assert m.header.last_position == prev_pos

    def test_set_resize_regular(self):
        cdef char * s = <char*>malloc(200)
        memset(s, 98, 200)
        s[199] = b'\0'

        cdef FIXBinaryMsg m = FIXBinaryMsg(<char> b'C', 200)
        assert m.header.last_position == 0
        self.assertEqual(strlen(s), 199)
        assert m.header.data_size == 200
        assert m.set(1, s, strlen(s)+1, b's') == 1
        self.assertEqual(m.header.n_reallocs, 1)
        self.assertEqual(m.header.data_size, 400)

        free(s)

    def test_set_resize_exact_fit(self):
        cdef int slen = 200
        cdef char * s = <char*>malloc(slen)
        memset(s, 98, slen)
        s[slen-1] = b'\0'

        # Exact match no resize
        cdef FIXBinaryMsg m = FIXBinaryMsg(<char> b'C', slen + sizeof(FIXRec))
        assert m.header.last_position == 0
        self.assertEqual(strlen(s), 199)
        assert m.header.data_size == slen + sizeof(FIXRec)
        assert m.set(1, s, slen, b's') == 1
        self.assertEqual(m.header.n_reallocs, 0)
        assert m.header.data_size == slen + sizeof(FIXRec)
        self.assertEqual(m.header.last_position, slen + sizeof(FIXRec))

        cdef char c = b'Z'
        assert m.set(2, &c, sizeof(char), b'c') == 1
        self.assertEqual(m.header.n_reallocs, 1)
        assert m.header.data_size == (slen + sizeof(FIXRec)) * 2

        free(s)

    def test_set_overflow_char(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        with self.assertRaises(OverflowError):
            m = FIXBinaryMsg(<char> b'C', 100000)

        m = FIXBinaryMsg(<char> b'@', USHRT_MAX)
        assert m.header.data_size == USHRT_MAX

        cdef unsigned char c
        cdef int i
        cdef void* value
        cdef uint16_t value_size
        prev_last_position = 0

        # Exclude zero tag error and tag 35 error
        max_records = int(USHRT_MAX / (sizeof(FIXRec) + sizeof(char))) + 2

        for i in range(0, USHRT_MAX):
            c = <char>(i % 255)
            assert c >= 0 and c < 255, f'{i} c={c}'
            if i == 0:
                self.assertEqual(m.set(i, &c, sizeof(char), b'c'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
            elif i == 35:
                self.assertEqual(m.set(i, &c, sizeof(char), b'c'), -4, f'{i}') # ERR_FIX_TAG35_NOTALLOWED
            else:
                if i < max_records:
                    self.assertEqual(m.set(i, &c, sizeof(char), b'c'), 1, f'{i}')
                    self.assertEqual(m.get(i, &value, &value_size, b'c'), 1, f'{i}')
                    #self.assertEqual((<char *> value)[0], c, i)
                    assert (<unsigned char *> value)[0] == c, i
                    assert value_size == 1
                    # Check for overflow
                    assert m.header.last_position > prev_last_position, i
                    self.assertEqual(int(m.header.last_position)-prev_last_position, sizeof(FIXRec) + sizeof(char))
                else:
                    self.assertEqual(m.set(i, &c, sizeof(char), b'c'), -6, f'{i}') # ERR_DATA_OVERFLOW
                    # Not found
                    self.assertEqual(m.get(i, &value, &value_size, b'c'), 0, f'{i}')
                    assert m.header.last_position == prev_last_position, i

                prev_last_position = m.header.last_position

        assert i == USHRT_MAX-1, i
        self.assertEqual(m.tag_hashmap.count(), max_records-2)
        assert m.header.n_reallocs == 0

        for i in range(0, USHRT_MAX):
            c = <char>(i % 255)
            if i == 0:
                self.assertEqual(m.get(i, &value, &value_size, b'c'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
                assert value == NULL
                assert value_size == 0
            elif i == 35:
                self.assertEqual(m.get(i, &value, &value_size, b'c'), 1, f'{i}')
                assert (<char*>value)[0] == b'@'
                assert value_size == 1
            else:
                if i < max_records:
                    self.assertEqual(m.get(i, &value, &value_size, b'c'), 1, f'{i}')
                    self.assertEqual((<unsigned char *> value)[0], c, f'i={i}  sizeof(FIXRec)={sizeof(FIXRec)}')
                    assert value_size == 1
                else:
                    self.assertEqual(m.get(i, &value, &value_size, b'c'), 0, f'{i}')


    def test_set_overflow_int(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        with self.assertRaises(OverflowError):
            m = FIXBinaryMsg(<char> b'C', 100000)

        m = FIXBinaryMsg(<char> b'@', USHRT_MAX)
        assert m.header.data_size == USHRT_MAX

        cdef int i
        cdef void* value
        cdef uint16_t value_size
        prev_last_position = 0

        # Exclude zero tag error and tag 35 error
        max_records = int(USHRT_MAX / (sizeof(FIXRec) + sizeof(int))) + 2

        for i in range(0, USHRT_MAX):
            if i == 0:
                self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
            elif i == 35:
                self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -4, f'{i}') # ERR_FIX_TAG35_NOTALLOWED
            else:
                if i < max_records:
                    self.assertEqual(m.set(i, &i, sizeof(int), b'i'), 1, f'{i}')
                    self.assertEqual(m.get(i, &value, &value_size, b'i'), 1, f'{i}')
                    assert (<int *> value)[0] == i, i
                    assert value_size == sizeof(int)
                    assert m.header.last_position > prev_last_position, i
                    self.assertEqual(int(m.header.last_position) - prev_last_position, sizeof(FIXRec) + sizeof(int))
                else:
                    self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -6, f'{i}')  # ERR_DATA_OVERFLOW
                    self.assertEqual(m.get(i, &value, &value_size, b'i'), 0, f'{i}')

                prev_last_position = m.header.last_position

        assert i == USHRT_MAX-1, i
        self.assertEqual(m.tag_hashmap.count(), max_records-2)
        assert m.header.n_reallocs == 0

        for i in range(0, USHRT_MAX):
            if i == 0:
                self.assertEqual(m.get(i, &value, &value_size, b'i'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
                assert value == NULL
                assert value_size == 0
            elif i == 35:
                self.assertEqual(m.get(i, &value, &value_size, b'i'), -2, f'{i}') # ERR_FIX_TYPE_MISMATCH
                assert value == NULL
                assert value_size == 0
            else:
                if i < max_records:
                    self.assertEqual(m.get(i, &value, &value_size, b'i'), 1)
                    self.assertEqual((<int *> value)[0], i, f'{i}')
                    assert value_size == sizeof(int)
                else:
                    self.assertEqual(m.get(i, &value, &value_size, b'i'), 0, f'{i}')


    def test_set_overflow_int_with_resize(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 20)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 20

        cdef int i
        cdef void* value
        cdef uint16_t value_size
        prev_last_position = 0

        # Exclude zero tag error and tag 35 error
        max_records = int(USHRT_MAX / (sizeof(FIXRec) + sizeof(int))) + 2

        for i in range(0, USHRT_MAX):
            if i == 0:
                self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
            elif i == 35:
                self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -4, f'{i}') # ERR_FIX_TAG35_NOTALLOWED
            else:
                if i < max_records:
                    self.assertEqual(m.set(i, &i, sizeof(int), b'i'), 1, f'{i}')
                    self.assertEqual(m.get(i, &value, &value_size, b'i'), 1, f'{i}')
                    assert (<int *> value)[0] == i, i
                    assert value_size == sizeof(int)
                    assert m.header.last_position > prev_last_position, i
                    self.assertEqual(int(m.header.last_position) - prev_last_position, sizeof(FIXRec) + sizeof(int))
                else:
                    self.assertEqual(m.set(i, &i, sizeof(int), b'i'), -6, f'{i}')  # ERR_DATA_OVERFLOW

                prev_last_position = m.header.last_position


    def test_group_start(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 20)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 20

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        assert g != NULL

        assert g.fix_rec.tag == 10
        assert g.fix_rec.value_type == b'\x07'
        assert g.fix_rec.value_len == sizeof(uint16_t) * 2 + sizeof(uint16_t)*3
        assert g.grp_n_elements == 2
        assert g.current_element == 0
        assert g.current_tag_len == -1
        assert g.n_tags == 3

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        assert fix_data_tags[0] == 1
        assert fix_data_tags[1] == 3
        assert fix_data_tags[2] == 4

        assert fix_data_el_offsets[0] == USHRT_MAX
        assert fix_data_el_offsets[1] == USHRT_MAX


    def test_group_add_tag(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 2000

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        assert g != NULL

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        cdef int val = 0
        cdef int base_len = sizeof(uint16_t) * 2 + sizeof(uint16_t)*3

        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef FIXRec * rec
        n_tags_added = 0
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(m.group_add_tag(10, i, &val, sizeof(int), b'i'), 1, f'i={i}')
                rec = <FIXRec*>(m.values + (m.header.last_position - sizeof(FIXRec) - sizeof(int)))
                val_data = (m.values + (m.header.last_position - sizeof(int)))
                n_tags_added += 1
                assert rec.tag == i
                assert rec.value_type == b'i'
                assert rec.value_len == sizeof(int)
                assert g.fix_rec.value_len == n_tags_added * (sizeof(FIXRec) + sizeof(int)) + base_len
                self.assertEqual((<int*>val_data)[0], val)

                #self.assertEqual(m.group_get(10, j, i, &val_data, &val_size, b'i'), 1, f'i={i} j={j}')
                #assert (<int *> val_data)[0] == val
            #
            if j == 0:
                assert fix_data_el_offsets[j] == m.header.last_position - 3 * (sizeof(FIXRec) + sizeof(int))
            else:
                assert fix_data_el_offsets[j]-fix_data_el_offsets[j-1] == 3 *  (sizeof(FIXRec) + sizeof(int))

            # First tag always must be in place
            rec = <FIXRec *> (m.values + fix_data_el_offsets[j])
            assert rec.tag == 1

        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + base_len)

        self.assertEqual(m.group_finish(10), 1)

        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + sizeof(FIXRec) + base_len)

        rec = <FIXRec *>(m.values + (m.header.last_position - sizeof(FIXRec)))
        assert rec.tag == 0
        assert rec.value_len == 0
        assert rec.value_type == b'\0'
        g = <GroupRec *>(m.values + m.header.last_position - g.fix_rec.value_len - sizeof(GroupRec))
        assert g.fix_rec.tag == 10

        assert m.open_group == NULL
        assert g != NULL

        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(m.group_get(10, j, i, &val_data, &val_size, b'i'), 1, f'i={i} j={j}')
                assert (<int*>val_data)[0] == val

    def test_group_finish(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 2000

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        assert g != NULL

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(GroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        cdef int val = 0
        cdef int base_len = sizeof(uint16_t) * 2 + sizeof(uint16_t)*3

        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef FIXRec * rec
        n_tags_added = 0
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(m.group_add_tag(10, i, &val, sizeof(int), b'i'), 1, f'i={i}')

        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + base_len)
        last_position = m.header.last_position
        self.assertEqual(m.group_finish(10), 1)

        # Make sure that last position and group size increased
        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + sizeof(FIXRec) + base_len)
        assert m.header.last_position == last_position + sizeof(FIXRec)

        rec = <FIXRec *> (m.values + (m.header.last_position - sizeof(FIXRec)))
        assert rec.tag == 0
        assert rec.value_len == 0
        assert rec.value_type == b'\0'
        assert m.open_group == NULL

    def test_group_start_errors__already_started(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), 1)
        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), -8) # ERR_GROUP_NOT_FINISHED

    def test_group_start_errors__already_started(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), 1)
        self.assertEqual(m.group_start(10, 2, 3, [1, 3, 4]), -8) # ERR_GROUP_NOT_FINISHED

    def test_group_start_errors__empty_group(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 0, 3, [1, 3, 4]), -9) #ERR_GROUP_EMPTY
        self.assertEqual(m.group_start(10, 1, 0, [1, 3, 4]), -9)  #ERR_GROUP_EMPTY

    def test_group_start_errors__too_many_tags(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 127, [1, 3, 4]), -13) #ERR_GROUP_TOO_MANY

    def test_group_start_errors__duplicate_tag_global_fixgrp(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        cdef int a = 1234
        m.set(10, &a, sizeof(int), b'i')
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), -1) #ERR_FIX_DUPLICATE_TAG

    def test_group_start_errors__duplicate_tag_global(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        cdef int a = 1234
        m.set(3, &a, sizeof(int), b'i')
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), -10) #ERR_GROUP_DUPLICATE_TAG

    def test_group_start_errors__duplicate_tag_group(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        # Duplicate between tag in members and group's tag
        self.assertEqual(m.group_start(10, 1, 3, [10, 3, 4]), -10) #ERR_GROUP_DUPLICATE_TAG

    def test_group_start_errors__duplicate_tag_members(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        # Duplicates inside members
        self.assertEqual(m.group_start(10, 1, 3, [3, 3, 4]), -10)  #ERR_GROUP_DUPLICATE_TAG

    def test_group_start_errors__tag_zero(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        # Duplicates inside members
        self.assertEqual(m.group_start(0, 1, 3, [1, 3, 4]), -5)  #ERR_FIX_ZERO_TAG
        self.assertEqual(m.group_start(1, 1, 3, [0, 3, 4]), -5)  #ERR_FIX_ZERO_TAG

    def test_group_add_tag_errors__not_started(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), -11) # ERR_GROUP_NOT_STARTED


    def test_group_add_tag_errors__grp_not_match(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(11, 1, &a, sizeof(int), b'i'), -12) # ERR_GROUP_NOT_MATCH

    def test_group_add_tag_errors__start_tag_expected(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), -14) # ERR_GROUP_START_TAG_EXPECTED

    def test_group_add_tag_errors__elements_overflow(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), -15) # ERR_GROUP_EL_OVERFLOW


    def test_group_add_tag_errors__tag_not_in_group(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 7, &a, sizeof(int), b'i'), -16) # ERR_GROUP_TAG_NOT_INGROUP

    def test_group_add_tag_errors__tag_wrong_order_and_duplicates(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &a, sizeof(int), b'i'), -10)# ERR_GROUP_DUPLICATE_TAG
        self.assertEqual(m.group_add_tag(10, 2, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER

    def test_group_add_tag_errors__tag_wrong_order_and_duplicates_2nd_grp(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &a, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &a, sizeof(int), b'i'), -10)  # ERR_GROUP_DUPLICATE_TAG
        self.assertEqual(m.group_add_tag(10, 2, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER

    def test_group_add_tag_zero(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 0, &a, sizeof(int), b'i'), -5) # ERR_FIX_ZERO_TAG
        self.assertEqual(m.group_add_tag(0, 1, &a, sizeof(int), b'i'), -5)  # ERR_FIX_ZERO_TAG

    def test_group_finish_errors(self):
        # Exact match no resize
        cdef FIXBinaryMsg m

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_finish(10), -11) #ERR_GROUP_NOT_STARTED

        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        self.assertEqual(m.group_finish(11), -12)  #ERR_GROUP_NOT_MATCH
        self.assertEqual(m.group_finish(10), -17)  #ERR_GROUP_NOT_COMPLETED

        cdef int a = 1234
        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_finish(10), -17)  #ERR_GROUP_NOT_COMPLETED

        self.assertEqual(m.group_add_tag(10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &a, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_finish(10), 1)  # Success
        assert m.open_group == NULL

    def test_group_get_errors__not_finished(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(m.group_add_tag(10, i, &val, sizeof(int), b'i'), 1, f'i={i}')

            self.assertEqual(m.group_get(10, j, 1, &val_data, &val_size, b'i'), -8) # ERR_GROUP_NOT_FINISHED

    def test_group_get_errors__not_found(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        self.assertEqual(m.group_get(101, 0, 1, &val_data, &val_size, b'i'), 0) # ERR_NOT_FOUND

    def test_group_get_errors__tag_zero(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        self.assertEqual(m.group_get(0, 1, 1, &val_data, &val_size, b'i'), -5) # ERR_FIX_ZERO_TAG
        self.assertEqual(m.group_get(10, 1, 0, &val_data, &val_size, b'i'), -5)  # ERR_FIX_ZERO_TAG


    def test_group_get_errors__fix_rec_type_mismach(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        # Corrupting fix rec type!
        g.fix_rec.value_type = b'w'
        self.assertEqual(m.group_get(10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED


    def test_group_get_errors__el_out_of_bounds(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        self.assertEqual(m.group_get(10, 2, 1, &val_data, &val_size, b'i'), -15)  # ERR_GROUP_EL_OVERFLOW

    def test_group_get_errors__tag_type_mismatch(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        self.assertEqual(m.group_get(10, 0, 1, &val_data, &val_size, b'c'), -2)  # ERR_FIX_TYPE_MISMATCH

    def test_group_get_errors__tag_not_in_group(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 2, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1

        self.assertEqual(m.group_get(10, 0, 5, &val_data, &val_size, b'i'), -16)  # ERR_GROUP_TAG_NOT_INGROUP

    def test_group_get_errors__tag_not_found(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 3, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &val, sizeof(int), b'i'), 1)

        assert m.group_finish(10) == 1
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(GroupRec) + g.n_tags * sizeof(uint16_t))
        for i in range(3):
            assert fix_data_el_offsets[i] < 1000


        self.assertEqual(m.group_get(10, 0, 1, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 0, 2, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 0, 3, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 0, 4, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(m.group_get(10, 1, 1, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 1, 2, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(m.group_get(10, 1, 3, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(m.group_get(10, 1, 4, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 2, 1, &val_data, &val_size, b'i'), 1)  # OK

        self.assertEqual(m.group_get(10, 2, 2, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(m.group_get(10, 2, 3, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(m.group_get(10, 2, 4, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND

        assert val_data == NULL
        assert val_size == 0

    def test_group_get_errors__corrupted_offset(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &val, sizeof(int), b'i'), 1)
        assert m.group_finish(10) == 1

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(GroupRec) + g.n_tags * sizeof(uint16_t))
        fix_data_el_offsets[0] = USHRT_MAX - 1

        self.assertEqual(m.group_get(10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED

    def test_group_get_errors__corrupted_start_tag(self):
        # Exact match no resize
        cdef FIXBinaryMsg m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXBinaryMsg(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000)
        self.assertEqual(m.group_start(10, 1, 4, [1, 2, 3, 4]), 1)
        cdef GroupRec * g = m.open_group
        val = 123
        self.assertEqual(m.group_add_tag(10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(m.group_add_tag(10, 3, &val, sizeof(int), b'i'), 1)
        assert m.group_finish(10) == 1

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(GroupRec) + g.n_tags * sizeof(uint16_t))

        cdef FIXRec *trec = <FIXRec *> (m.values + fix_data_el_offsets[0])
        trec.tag = 5

        self.assertEqual(m.group_get(10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED
