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
