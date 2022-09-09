import time
import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy, datetime_nsnow
from libc.stdint cimport uint64_t, uint16_t, int8_t
from libc.string cimport memcmp, strlen, strcmp, memcpy, memset
from libc.stdlib cimport malloc, free
from uberhf.prototols.messages cimport *
from uberhf.includes.asserts cimport cybreakpoint
import os
import pytest
from libc.limits cimport USHRT_MAX, UCHAR_MAX

from uberhf.orders.fix_msg cimport FIXRec, FIXMsg, FIXMsgStruct, FIXHeader, FIXGroupRec, FIXOffsetMap


cdef FIXMsgStruct * make_almost_overflowed(int n_bytes_remaining):
    """
    Makes FIXMsgStruct with 5 bytes left before overflow
    :return: 
    """
    #cybreakpoint(1)
    assert n_bytes_remaining <= 1000, 'Not implemented for large'
    cdef FIXMsgStruct * m = FIXMsg.create(<char> b'@', USHRT_MAX, UCHAR_MAX)

    assert m != NULL
    assert m.header.data_size == USHRT_MAX
    assert m.header.tags_capacity == UCHAR_MAX

    cdef int i
    cdef void * value
    cdef uint16_t value_size
    prev_last_position = 0

    cdef uint16_t buf_size = 1024 - sizeof(FIXRec)  # Exact 1024 bytes with header
    cdef char * val = <char *> malloc(buf_size)
    memset(val, 98, buf_size)
    val[buf_size - 1] = b'\0'
    assert strlen(val) == buf_size - 1, strlen(val)

    for i in range(0, USHRT_MAX):
        if i == 0:
            continue
        elif i == 35:
            continue
        else:
            if m.header.tags_count < 63:
                assert FIXMsg.set(m, i, val, buf_size, b's') ==  1, f'{m.header.tags_count}'
                assert FIXMsg.get(m, i, &value, &value_size, b's') ==  1, f'{i}'
                assert value_size == buf_size
                assert strcmp(val, <char *> value) == 0
                assert m.header.last_position > prev_last_position, i
                assert FIXMsg.is_valid(m) == 1, i
            else:
                break

    # No bytes left in the buffer
    assert m.header.data_size, USHRT_MAX
    assert m.header.last_position, 64512

    #
    # Make string shorter
    val[buf_size - 1 - n_bytes_remaining - 1] = b'\0'
    assert FIXMsg.set(m, 100, val, strlen(val) + 1, b's') == 1, f'{m.header.tags_count}'  # OK
    assert m.header.last_position, 65531  # Real bytes written
    free(val)
    #cybreakpoint(1)
    return m


# noinspection PyTypeChecker
class CyFIXStaticMsgTestCase(unittest.TestCase):
    def test_size_of_structs(self):
        self.assertEqual(18, sizeof(FIXHeader))
        self.assertEqual(4, sizeof(FIXOffsetMap))
        self.assertEqual(6, sizeof(FIXRec))
        self.assertEqual(14, sizeof(FIXGroupRec))

    def test_init_msg(self):
        # Zero data size
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 0, 10)
        assert m == NULL

        # Zero tags
        m = FIXMsg.create(<char> b'C', 10, 0)
        assert m == NULL

        m = FIXMsg.create(<char> b'C', 128, 10)
        assert m != NULL

        assert m.header.data_size == 128
        assert m.header.msg_type == b'C'
        assert m.header.magic_number == 22093
        assert m.header.last_position == 0
        assert m.header.n_reallocs == 0
        assert m.header.tag_errors == 0
        assert m.open_group == NULL
        assert FIXMsg.is_valid(m) == 1
        assert m.header.is_read_only == 0
        FIXMsg.destroy(m)


        m = FIXMsg.create(<char> b'C', 1000, 64)

        assert m.header.data_size == 1000
        assert m.header.tags_count == 0
        assert m.header.tags_capacity == 64
        assert FIXMsg.is_valid(m) == 1
        FIXMsg.destroy(m)

    def test_get_set_tag_index(self):
        m = FIXMsg.create(<char> b'C', 1000, 64)
        assert m != NULL

        assert FIXMsg._set_tag_offset(m, 1, 10) == 0
        assert m.header.tags_last == 1
        assert m.header.tags_last_idx == 0
        assert FIXMsg._set_tag_offset(m, 2, 20) == 1
        assert m.header.tags_last == 2
        assert m.header.tags_last_idx == 1
        assert FIXMsg._set_tag_offset(m, 3, 30) == 2
        assert m.header.tags_last == 3
        assert m.header.tags_last_idx == 2

        assert m.tags[0].tag == 1
        assert m.tags[1].tag == 2
        assert m.tags[2].tag == 3
        assert m.tags[0].data_offset == 10
        assert m.tags[1].data_offset == 20
        assert m.tags[2].data_offset == 30

        self.assertEqual(FIXMsg._get_tag_offset(m, 1), 10)
        self.assertEqual(FIXMsg._get_tag_offset(m, 2), 20)
        self.assertEqual(FIXMsg._get_tag_offset(m, 3), 30)

        assert FIXMsg.is_valid(m) == 1

        FIXMsg.destroy(m)

    def test_get_set_tag_index_random_insert_and_search(self):
        m = FIXMsg.create(<char> b'C', 1000, 64)
        assert m != NULL

        assert FIXMsg._set_tag_offset(m, 3, 30) == 0
        assert FIXMsg._set_tag_offset(m, 6, 60) == 1
        assert FIXMsg._set_tag_offset(m, 8, 80) == 2

        assert FIXMsg._set_tag_offset(m, 2, 20) == 0
        assert FIXMsg._set_tag_offset(m, 4, 40) == 2
        assert FIXMsg._set_tag_offset(m, 7, 70) == 4



        self.assertEqual(FIXMsg._get_tag_offset(m, 2), 20)
        self.assertEqual(FIXMsg._get_tag_offset(m, 3), 30)
        self.assertEqual(FIXMsg._get_tag_offset(m, 4), 40)
        self.assertEqual(FIXMsg._get_tag_offset(m, 6), 60)
        self.assertEqual(FIXMsg._get_tag_offset(m, 7), 70)
        self.assertEqual(FIXMsg._get_tag_offset(m, 8), 80)

        self.assertEqual(FIXMsg._get_tag_offset(m, 1), 65534) #DEF TAG_NOT_FOUND = 	65534 # USHRT_MAX-1
        self.assertEqual(FIXMsg._get_tag_offset(m, 5), 65534)  #DEF TAG_NOT_FOUND = 	65534 # USHRT_MAX-1
        self.assertEqual(FIXMsg._get_tag_offset(m, 9), 65534)  #DEF TAG_NOT_FOUND = 	65534 # USHRT_MAX-1


        FIXMsg.destroy(m)


    def test_get_set_tag_index_duplicate_tags(self):
        m = FIXMsg.create(<char> b'C', 1000, 64)
        assert m != NULL

        assert FIXMsg._set_tag_offset(m, 3, 30) == 0
        assert FIXMsg._set_tag_offset(m, 3, 60) == 65533  #DEF TAG_DUPLICATE = 	65533 # USHRT_MAX-2

        self.assertEqual(FIXMsg._get_tag_offset(m, 3), 65533) #DEF TAG_DUPLICATE = 	65533 # USHRT_MAX-2
        assert FIXMsg.is_valid(m) == 0

        FIXMsg.destroy(m)

    def test_get_set_tag_index_overflow(self):
        m = FIXMsg.create(<char> b'C', 1000, 1)
        assert m != NULL

        assert FIXMsg._set_tag_offset(m, 3, 30) == 0
        assert FIXMsg._set_tag_offset(m, 6, 60) == 65532  #DEF TAG_NEED_RESIZE = 	65532 # USHRT_MAX-3

        assert FIXMsg.is_valid(m) == 0 # Without preliminary handling it will invalidate message!
        FIXMsg.destroy(m)

    def test_get_set_raw(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 2000, 100)
        assert m != NULL
        cdef int value = 123

        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert m.header.tags_count == 1
        assert FIXMsg.is_valid(m) == 1

        # Not allowed type
        assert FIXMsg.set(m, 13, &value, sizeof(int), b'\0') == -4
        assert FIXMsg.set(m, 14, &value, sizeof(int), b'\x07') == -4
        assert FIXMsg.is_valid(m) == 0

        cdef void * p_value = NULL
        cdef uint16_t p_size = 0

        self.assertEqual(FIXMsg.get(m, 11, &p_value, &p_size, b'i'), 1)
        self.assertEqual((<int*>p_value)[0], 123)
        self.assertEqual(p_size, sizeof(int))

        # Not found
        self.assertEqual(FIXMsg.get(m, 1, &p_value, &p_size, b'i'), 0)
        assert p_value == NULL
        assert p_size == 0

        # Type mismatch
        self.assertEqual(FIXMsg.get(m, 11, &p_value, &p_size, b'c'), -2)
        assert p_value == NULL
        assert p_size == 0

        # Type not allowed
        self.assertEqual(FIXMsg.get(m, 11, &p_value, &p_size, b'\0'), -4)
        assert p_value == NULL
        assert p_size == 0

        self.assertEqual(FIXMsg.get(m, 11, &p_value, &p_size, b'\x07'), -4)
        assert p_value == NULL
        assert p_size == 0
        FIXMsg.destroy(m)

    def test_set_resize(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 10, 2)
        assert m != NULL
        cdef int value = 123
        assert sizeof(int) + sizeof(FIXRec) == 10
        assert FIXMsg.is_valid(m) == 1
        assert FIXMsg.has_capacity(m, 2, 10) == 1
        assert FIXMsg.has_capacity(m, 3, 10) == -21
        self.assertEqual(FIXMsg.has_capacity(m, 1, 10), 1)
        assert FIXMsg.has_capacity(m, 2, 11) == -21


        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert m.header.tags_count == 1
        assert m.header.tags_capacity == 2
        assert m.header.data_size == 10
        assert m.header.last_position == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.has_capacity(m, 1, 10) == -21 # ERR_RESIZE_REQUIRED

        cdef FIXMsgStruct * m2 = FIXMsg.resize(m, 0, 10)
        assert m2 != NULL
        m = m2  # m may not be a valid pointer anymore!

        # Getting reject, but message is still valid.
        # Giving a chance to user code to resize it
        assert FIXMsg.set(m, 12, &value, sizeof(int), b'i') ==  1
        assert FIXMsg.is_valid(m) == 1

        FIXMsg.destroy(m)

    def test_set_exact_capacity(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char> b'C', 10, 1)
        assert m != NULL
        cdef int value = 123
        assert sizeof(int) + sizeof(FIXRec) == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert m.header.tags_count == 1
        assert m.header.tags_capacity == 1
        assert m.header.data_size == 10
        assert m.header.last_position == 10
        assert FIXMsg.is_valid(m) == 1

        assert m.tags[0].tag == 11
        assert m.tags[0].data_offset == 0
        cdef FIXRec * rec = <FIXRec *> (m.values + sizeof(FIXRec) * 0)
        assert rec.tag == 11
        assert rec.value_len == 4
        assert rec.value_type == b'i'

        assert FIXMsg.has_capacity(m, 1, 10) == -21 # ERR_TAG_RESIZE_REQUIRED

        # Getting reject, but message is still valid.
        # Giving a chance to user code to resize it
        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == -21  # ERR_TAG_RESIZE_REQUIRED
        assert FIXMsg.is_valid(m) == 1

        FIXMsg.destroy(m)

    def test_set_resize_only_tags(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 20, 1)
        assert m != NULL
        cdef int value = 123
        assert sizeof(int) + sizeof(FIXRec) == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert m.header.tags_count == 1
        assert m.header.tags_capacity == 1
        assert m.header.data_size == 20
        assert m.header.last_position == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.has_capacity(m, 1, 10) == -21

        cdef FIXMsgStruct * m2 = FIXMsg.resize(m, 1, 0)
        assert m2 != NULL
        m = m2  # m may not be a valid pointer anymore!
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.set(m, 12, &value, sizeof(int), b'i') ==  1
        assert FIXMsg.is_valid(m) == 1
        assert m.header.tags_count == 2
        assert m.header.tags_capacity == 2
        assert m.header.data_size == 20
        assert m.header.last_position == 20

        FIXMsg.destroy(m)

    def test_set_resize_both_tags_data(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 10, 1)
        assert m != NULL
        cdef int value = 123
        assert sizeof(int) + sizeof(FIXRec) == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert m.header.tags_count == 1
        assert m.header.tags_capacity == 1
        assert m.header.data_size == 10
        assert m.header.last_position == 10
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.has_capacity(m, 1, 10) == -21 # ERR_TAG_RESIZE_REQUIRED
        cdef FIXMsgStruct * m2 = FIXMsg.resize(m, 1, 10)
        assert m2 != NULL
        m = m2  # m may not be a valid pointer anymore!
        assert m.header.n_reallocs == 1
        assert FIXMsg.is_valid(m) == 1

        assert FIXMsg.set(m, 12, &value, sizeof(int), b'i') ==  1
        assert FIXMsg.is_valid(m) == 1
        assert m.header.tags_count == 2
        assert m.header.tags_capacity == 2
        self.assertEqual(m.header.data_size, 20)
        assert m.header.last_position == 20

        FIXMsg.destroy(m)

    def test_set_value_size_too_long(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 1024 + sizeof(FIXRec), 1)
        cdef int value = 123
        cdef char * val = <char*>malloc(1024)
        memset(val, 98, 1024)
        val[1023] = b'\0'
        assert strlen(val) == 1023, strlen(val)
        self.assertEqual(FIXMsg.set(m, 10, val, 1024, b's'), 1) # OK
        assert FIXMsg.set(m, 11, &value, 1025, b'i') == -3 # ERR_FIX_VALUE_TOOLONG
        assert FIXMsg.set(m, 12, &value, 0, b'i') == -20 # ERR_UNEXPECTED_TYPE_SIZE
        free(val)

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)


    def test_get_set_duplicate(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 100, 4)
        cdef int value = 123
        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == 1
        assert FIXMsg.set(m, 11, &value, sizeof(int), b'i') == -1
        assert m.header.tags_count == 1
        assert m.header.tag_errors == 1

        cdef void * p_value = NULL
        cdef uint16_t p_size = 0

        # Get also return errors for duplicated tags
        self.assertEqual(FIXMsg.get(m, 11, &p_value, &p_size, b'i'), -1)
        assert p_value == NULL
        assert p_size == 0
        assert FIXMsg.is_valid(m) == 0
        FIXMsg.destroy(m)

    def test_get_set_multiple_base_types(self):
        cdef FIXMsgStruct * m = FIXMsg.create(<char>b'C', 200, 100)
        cdef int i = 123
        cdef double f = 8907.889
        cdef char c = b'V'
        cdef char * s = b'my fancy string'
        cdef FIXRec* rec
        cdef void * p_value = NULL
        cdef uint16_t p_size = 0

        cdef int prev_pos = m.header.last_position
        assert FIXMsg.set(m, 1, &i, sizeof(int), b'i') == 1

        rec = <FIXRec*>(m.values + prev_pos)
        assert rec.tag == 1
        assert rec.value_type == b'i'
        assert rec.value_len == sizeof(int)
        assert FIXMsg.get(m, 1, &p_value, &p_size, b'i') == 1
        assert p_size == sizeof(int)
        assert (<int*>p_value)[0] == i


        prev_pos = sizeof(FIXRec) + sizeof(int)
        assert m.header.last_position == prev_pos
        assert FIXMsg.set(m, 2, &f, sizeof(double), b'f') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 2
        assert rec.value_type == b'f'
        assert rec.value_len == sizeof(double)

        assert FIXMsg.get(m, 2, &p_value, &p_size, b'f') == 1
        assert p_size == sizeof(double)
        assert (<double *> p_value)[0] == f

        prev_pos += sizeof(FIXRec) + sizeof(double)
        assert m.header.last_position == prev_pos

        assert FIXMsg.set(m, 3, &c, sizeof(char), b'c') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 3
        assert rec.value_type == b'c'
        assert rec.value_len == sizeof(char)

        assert FIXMsg.get(m, 3, &p_value, &p_size, b'c') == 1
        assert p_size == sizeof(char)
        assert (<char *> p_value)[0] == c


        prev_pos += sizeof(FIXRec) + sizeof(char)
        assert m.header.last_position == prev_pos
        assert FIXMsg.set(m, 4, s, strlen(s)+1, b's') == 1
        rec = <FIXRec *> (m.values + prev_pos)
        assert rec.tag == 4
        assert rec.value_type == b's'
        assert rec.value_len == strlen(s)+1

        assert FIXMsg.get(m, 4, &p_value, &p_size, b's') == 1
        assert p_size == strlen(s)+1
        assert strcmp((<char*>p_value), s) == 0

        prev_pos += sizeof(FIXRec) + strlen(s)+1
        assert m.header.last_position == prev_pos

        assert FIXMsg.is_valid(m, ) == 1
        FIXMsg.destroy(m)

    def test_set_overflow_char(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', USHRT_MAX, UCHAR_MAX)
        assert m.header.data_size == USHRT_MAX
        assert m.header.tags_capacity == UCHAR_MAX

        cdef unsigned char c
        cdef int i
        cdef void* value
        cdef uint16_t value_size
        prev_last_position = 0

        # Exclude zero tag error and tag 35 error
        max_records = int(USHRT_MAX / (sizeof(FIXRec) + sizeof(char))) + 1

        for i in range(0, USHRT_MAX):
            c = <char>(i % 255)
            assert c >= 0 and c < 255, f'{i} c={c}'
            if i == 0:
                #self.assertEqual(FIXMsg.set(m, i, &c, sizeof(char), b'c'), -5, f'{i}')  # ERR_FIX_ZERO_TAG
                pass
            elif i == 35:
                #self.assertEqual(FIXMsg.set(m, i, &c, sizeof(char), b'c'), -4, f'{i}') # ERR_FIX_TAG35_NOTALLOWED
                pass
            else:
                if m.header.tags_count < 255:
                    self.assertEqual(FIXMsg.set(m, i, &c, sizeof(char), b'c'), 1, f'{i} max_records={max_records}')
                    self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), 1, f'{i}')
                    #self.assertEqual((<char *> value)[0], c, i)
                    assert value != NULL
                    assert (<unsigned char *> value)[0] == c, i
                    assert value_size == 1
                    # Check for overflow
                    assert m.header.last_position > prev_last_position, i
                    self.assertEqual(int(m.header.last_position)-prev_last_position, sizeof(FIXRec) + sizeof(char))
                    assert FIXMsg.is_valid(m) == 1
                else:
                    self.assertEqual(FIXMsg.set(m, i, &c, sizeof(char), b'c'), -6, f'{i}') # ERR_DATA_OVERFLOW
                    if i >= USHRT_MAX - 10:
                        self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), -6, f'{i}')
                    else:
                        self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), -6, f'{i}')
                    self.assertEqual(m.header.last_position, USHRT_MAX)
                    assert FIXMsg.is_valid(m) == 0

                prev_last_position = m.header.last_position

        assert FIXMsg.is_valid(m, ) == 0
        assert i == USHRT_MAX-1, i
        self.assertEqual(m.header.tags_count, 255)
        assert m.header.n_reallocs == 0

        for i in range(0, USHRT_MAX):

            c = <char>(i % 255)
            if i == 0:
                self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), -6, f'{i}')  # OVERFLOW
                assert value == NULL
                assert value_size == 0
            elif i == 35:
                self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), -6, f'{i}')
                assert value == NULL
                assert value_size == 0

            else:
                self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b'c'), -6, f'{i}')
                assert value == NULL
                assert value_size == 0

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_set_overflow_string(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', USHRT_MAX, UCHAR_MAX)
        assert m.header.data_size == USHRT_MAX
        assert m.header.tags_capacity == UCHAR_MAX

        cdef int i
        cdef void* value
        cdef uint16_t value_size
        prev_last_position = 0

        cdef uint16_t buf_size = 1024 - sizeof(FIXRec) # Exact 1024 bytes with header
        cdef char * val = <char *> malloc(buf_size)
        memset(val, 98, buf_size)
        val[buf_size-1] = b'\0'
        assert strlen(val) == buf_size-1, strlen(val)


        for i in range(0, USHRT_MAX):
            if i == 0:
                continue
            elif i == 35:
                continue
            else:
                if m.header.tags_count < 63:
                    self.assertEqual(FIXMsg.set(m, i, val, buf_size, b's'), 1, f'{m.header.tags_count}')  # OK
                    self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b's'), 1, f'{i}')
                    assert value_size == buf_size
                    assert strcmp(val, <char*>value) == 0
                    assert m.header.last_position > prev_last_position, i
                    assert FIXMsg.is_valid(m) == 1, i
                else:
                    self.assertEqual(FIXMsg.set(m, i, val, buf_size, b's'), -6, f'{i}')  # ERR_DATA_OVERFLOW
                    self.assertEqual(FIXMsg.get(m, i, &value, &value_size, b's'), -6, f'{i}')
                    assert FIXMsg.is_valid(m) == 0, i

                prev_last_position = m.header.last_position

        assert i == USHRT_MAX-1, i
        self.assertEqual(m.header.tags_count, 63)
        assert m.header.n_reallocs == 0

        free(val)
        FIXMsg.destroy(m)

    def test_group_start(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 20, 20)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 20

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        assert g != NULL

        assert g.fix_rec.tag == 10
        assert g.fix_rec.value_type == b'\x07'
        assert g.fix_rec.value_len == sizeof(uint16_t) * 2 + sizeof(uint16_t)*3
        assert g.grp_n_elements == 2
        assert g.current_element == 0
        assert g.current_tag_len == -1
        assert g.n_tags == 3

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        assert fix_data_tags[0] == 1
        assert fix_data_tags[1] == 3
        assert fix_data_tags[2] == 4

        assert fix_data_el_offsets[0] == USHRT_MAX
        assert fix_data_el_offsets[1] == USHRT_MAX

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)


    def test_group_add_tag(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, UCHAR_MAX)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 2000

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        assert g != NULL

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        cdef int val = 0
        cdef int base_len = sizeof(uint16_t) * 2 + sizeof(uint16_t)*3

        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef FIXRec * rec
        n_tags_added = 0
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(FIXMsg.group_add_tag(m, 10, i, &val, sizeof(int), b'i'), 1, f'i={i}')
                rec = <FIXRec*>(m.values + (m.header.last_position - sizeof(FIXRec) - sizeof(int)))
                val_data = (m.values + (m.header.last_position - sizeof(int)))
                n_tags_added += 1
                assert rec.tag == i
                assert rec.value_type == b'i'
                assert rec.value_len == sizeof(int)
                assert g.fix_rec.value_len == n_tags_added * (sizeof(FIXRec) + sizeof(int)) + base_len
                self.assertEqual((<int*>val_data)[0], val)

                #self.assertEqual(FIXMsg.group_get(m, 10, j, i, &val_data, &val_size, b'i'), 1, f'i={i} j={j}')
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

        assert FIXMsg.is_valid(m, ) == 0 # Group not finished
        self.assertEqual(FIXMsg.group_finish(m, 10), 1)
        assert FIXMsg.is_valid(m, ) == 1 # All good

        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + sizeof(FIXRec) + base_len)

        rec = <FIXRec *>(m.values + (m.header.last_position - sizeof(FIXRec)))
        assert rec.tag == 0
        assert rec.value_len == 0
        assert rec.value_type == b'\0'
        g = <FIXGroupRec *>(m.values + m.header.last_position - g.fix_rec.value_len - sizeof(FIXGroupRec))
        assert g.fix_rec.tag == 10

        assert m.open_group == NULL
        assert g != NULL

        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(FIXMsg.group_get(m, 10, j, i, &val_data, &val_size, b'i'), 1, f'i={i} j={j}')
                assert (<int*>val_data)[0] == val

        FIXMsg.destroy(m)

    def test_group_finish(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, UCHAR_MAX)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 2000

        cdef int i
        cdef void* value
        cdef uint16_t value_size

        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), 1)

        self.assertEqual(FIXMsg.group_finish(m, 10), -17) # ERR_GROUP_NOT_COMPLETED

        cdef FIXGroupRec * g = m.open_group
        assert g != NULL

        cdef uint16_t *fix_data_tags = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec))
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> m.open_group + sizeof(FIXGroupRec) + m.open_group.n_tags * sizeof(uint16_t))

        cdef int val = 0
        cdef int base_len = sizeof(uint16_t) * 2 + sizeof(uint16_t)*3

        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef FIXRec * rec
        n_tags_added = 0
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(FIXMsg.group_add_tag(m, 10, i, &val, sizeof(int), b'i'), 1, f'i={i}')

        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + base_len)
        last_position = m.header.last_position
        self.assertEqual(FIXMsg.group_finish(m, 10), 1)

        # Make sure that last position and group size increased
        self.assertEqual(g.fix_rec.value_len, 6 * (sizeof(FIXRec) + sizeof(int)) + sizeof(FIXRec) + base_len)
        assert m.header.last_position == last_position + sizeof(FIXRec)

        rec = <FIXRec *> (m.values + (m.header.last_position - sizeof(FIXRec)))
        assert rec.tag == 0
        assert rec.value_len == 0
        assert rec.value_type == b'\0'
        assert m.open_group == NULL

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), 1, f'i={i}')
        FIXMsg.destroy(m)

    def test_group_start_errors__already_started(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), 1)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), -8) # ERR_GROUP_NOT_FINISHED
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__already_started(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), 1)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 3, [1, 3, 4]), -8) # ERR_GROUP_NOT_FINISHED
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__empty_group(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 0, 3, [1, 3, 4]), -9) #ERR_GROUP_EMPTY
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 0, [1, 3, 4]), -9)  #ERR_GROUP_EMPTY
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__too_many_tags(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 127, [1, 3, 4]), -13) #ERR_GROUP_TOO_MANY
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__duplicate_tag_global_fixgrp(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        cdef int a = 1234
        FIXMsg.set(m, 10, &a, sizeof(int), b'i')
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), -1) #ERR_FIX_DUPLICATE_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__duplicate_tag_global(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        cdef int a = 1234
        FIXMsg.set(m, 3, &a, sizeof(int), b'i')
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), -10) #ERR_GROUP_DUPLICATE_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__duplicate_tag_group(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        # Duplicate between tag in members and group's tag
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [10, 3, 4]), -10) #ERR_GROUP_DUPLICATE_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__duplicate_tag_members(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        # Duplicates inside members
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [3, 3, 4]), -10)  #ERR_GROUP_DUPLICATE_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_start_errors__tag_zero(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        # Duplicates inside members
        self.assertEqual(FIXMsg.group_start(m, 0, 1, 3, [1, 3, 4]), -5)  #ERR_FIX_ZERO_TAG
        self.assertEqual(FIXMsg.group_start(m, 1, 1, 3, [0, 3, 4]), -5)  #ERR_FIX_ZERO_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_errors__not_started(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), -11) # ERR_GROUP_NOT_STARTED
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)


    def test_group_add_tag_errors__grp_not_match(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 11, 1, &a, sizeof(int), b'i'), -12) # ERR_GROUP_NOT_MATCH
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_errors__start_tag_expected(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), -14) # ERR_GROUP_START_TAG_EXPECTED
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_errors__elements_overflow(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), -15) # ERR_GROUP_EL_OVERFLOW
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)


    def test_group_add_tag_errors__tag_not_in_group(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 3, [1, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 7, &a, sizeof(int), b'i'), -16) # ERR_GROUP_TAG_NOT_INGROUP
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_errors__tag_wrong_order_and_duplicates(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &a, sizeof(int), b'i'), -10)# ERR_GROUP_DUPLICATE_TAG
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_errors__tag_wrong_order_and_duplicates_2nd_grp(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &a, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &a, sizeof(int), b'i'), -10)  # ERR_GROUP_DUPLICATE_TAG
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), -18)  # ERR_GROUP_TAG_WRONG_ORDER
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_zero(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 0, &a, sizeof(int), b'i'), -5) # ERR_FIX_ZERO_TAG
        self.assertEqual(FIXMsg.group_add_tag(m, 0, 1, &a, sizeof(int), b'i'), -5)  # ERR_FIX_ZERO_TAG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_too_long(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, 1025, b'i'), -3) # ERR_FIX_VALUE_TOOLONG
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_size_zero(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, 0, b'i'), -20)# ERR_UNEXPECTED_TYPE_SIZE
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_add_tag_string_length_mismatch(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        cdef void *value
        cdef uint16_t value_size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef char * s = b'12345'
        # This is not reliable test!
        #self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &s, strlen(s), b's'), -20)# ERR_UNEXPECTED_TYPE_SIZE
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, s, strlen(s), b's'), -20)  # ERR_UNEXPECTED_TYPE_SIZE
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)


    def test_group_add_tag_type_not_allowed(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'\0'), -4) # ERR_FIX_NOT_ALLOWED
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &a, sizeof(int), b'\x07'), -4)  # ERR_FIX_NOT_ALLOWED
        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_finish_errors(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void *value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_finish(m, 10), -11) #ERR_GROUP_NOT_STARTED

        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        self.assertEqual(FIXMsg.group_finish(m, 11), -12)  #ERR_GROUP_NOT_MATCH
        self.assertEqual(FIXMsg.group_finish(m, 10), -17)  #ERR_GROUP_NOT_COMPLETED

        cdef int a = 1234
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), 1)

        #
        # Get / set also rejected when the group is open
        #
        self.assertEqual(FIXMsg.set(m, 123, &a, sizeof(int), b'i'), -8 ) # ERR_GROUP_NOT_FINISHED
        self.assertEqual(FIXMsg.get(m, 123, &value, &size, b'i'), -8)  # ERR_GROUP_NOT_FINISHED
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 1, &value, &size, b'i'), -8)
        self.assertEqual(FIXMsg.group_count(m, 10), -8)

        self.assertEqual(FIXMsg.group_finish(m, 10), -17)  #ERR_GROUP_NOT_COMPLETED

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &a, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_finish(m, 10), 1)  # Success
        assert m.open_group == NULL

        assert m.header.tag_errors > 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_get_errors__not_finished(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        for j in range(2):
            for i in [1, 3, 4]:
                val = (j+1) * 100 + i
                self.assertEqual(FIXMsg.group_add_tag(m, 10, i, &val, sizeof(int), b'i'), 1, f'i={i}')

            self.assertEqual(FIXMsg.group_get(m, 10, j, 1, &val_data, &val_size, b'i'), -8) # ERR_GROUP_NOT_FINISHED
            self.assertEqual(FIXMsg.group_count(m, 10), -8) # ERR_GROUP_NOT_FINISHED
        assert m.header.tag_errors == 0
        assert FIXMsg.is_valid(m, ) == 0  # Had errors
        FIXMsg.destroy(m)

    def test_group_get_errors__not_found(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        self.assertEqual(FIXMsg.group_get(m, 101, 0, 1, &val_data, &val_size, b'i'), 0) # ERR_NOT_FOUND

        self.assertEqual(FIXMsg.group_count(m, 101), 0)  # ERR_NOT_FOUND
        FIXMsg.destroy(m)

    def test_group_get_errors__duplicate_tag_or_overflow(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)
        assert FIXMsg.group_finish(m, 10) == 1

        # All good
        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), 1)

        # Oops tag dupe
        assert FIXMsg.set(m, 10, &val, sizeof(int), b'i') == -1

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), -1) # ERR_FIX_DUPLICATE_TAG

        self.assertEqual(FIXMsg.group_get(m, USHRT_MAX-10, 0, 1, &val_data, &val_size, b'i'), -6)  # ERR_DATA_OVERFLOW

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_group_get_errors__tag_zero_or_not_allowed(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        self.assertEqual(FIXMsg.group_get(m, 0, 1, 1, &val_data, &val_size, b'i'), -5) # ERR_FIX_ZERO_TAG
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 0, &val_data, &val_size, b'i'), -5)  # ERR_FIX_ZERO_TAG
        self.assertEqual(FIXMsg.group_get(m, 35, 1, 1, &val_data, &val_size, b'i'), -4)  # ERR_FIX_NOT_ALLOWED

        self.assertEqual(FIXMsg.group_count(m, 0), -5)  # ERR_FIX_ZERO_TAG
        self.assertEqual(FIXMsg.group_count(m, 35), -4)  # ERR_FIX_NOT_ALLOWED

        # Read operation doesn't trigger msg corruption!
        assert FIXMsg.is_valid(m, ) == 1
        FIXMsg.destroy(m)


    def test_group_get_errors__fix_rec_type_mismach(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        # Corrupting fix rec type!
        g.fix_rec.value_type = b'w'
        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED
        self.assertEqual(FIXMsg.group_count(m, 10), -19)  # ERR_GROUP_CORRUPTED

        # Read operation doesn't trigger msg corruption! -- except ERR_GROUP_CORRUPTED
        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)


    def test_group_get_errors__el_out_of_bounds(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        self.assertEqual(FIXMsg.group_get(m, 10, 2, 1, &val_data, &val_size, b'i'), -15)  # ERR_GROUP_EL_OVERFLOW
        FIXMsg.destroy(m)

    def test_group_get_errors__tag_type_mismatch(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'c'), -2)  # ERR_FIX_TYPE_MISMATCH
        FIXMsg.destroy(m)

    def test_group_get_errors__tag_not_in_group(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void* val_data = NULL
        cdef uint16_t val_size = 0
        cdef int val = 0

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 2, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 5, &val_data, &val_size, b'i'), -16)  # ERR_GROUP_TAG_NOT_INGROUP
        FIXMsg.destroy(m)

    def test_group_get_errors__tag_not_found(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 3, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 4, &val, sizeof(int), b'i'), 1)

        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &val, sizeof(int), b'i'), 1)

        assert FIXMsg.group_finish(m, 10) == 1
        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(FIXGroupRec) + g.n_tags * sizeof(uint16_t))
        for i in range(3):
            assert fix_data_el_offsets[i] < 1000


        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 0, 2, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 0, 3, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 0, 4, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 1, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 2, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 3, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(FIXMsg.group_get(m, 10, 1, 4, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 2, 1, &val_data, &val_size, b'i'), 1)  # OK

        self.assertEqual(FIXMsg.group_get(m, 10, 2, 2, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND
        self.assertEqual(FIXMsg.group_get(m, 10, 2, 3, &val_data, &val_size, b'i'), 1)  # OK
        self.assertEqual(FIXMsg.group_get(m, 10, 2, 4, &val_data, &val_size, b'i'), 0)  # ERR_NOT_FOUND

        assert val_data == NULL
        assert val_size == 0
        FIXMsg.destroy(m)

    def test_group_get_errors__corrupted_offset(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &val, sizeof(int), b'i'), 1)
        assert FIXMsg.group_finish(m, 10) == 1
        assert FIXMsg.is_valid(m, ) == 1

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(FIXGroupRec) + g.n_tags * sizeof(uint16_t))
        fix_data_el_offsets[0] = USHRT_MAX - 1

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED
        # Read operation doesn't trigger msg corruption! -- except ERR_GROUP_CORRUPTED
        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_group_get_errors__corrupted_start_tag(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef int val = 0
        cdef void* val_data = &val
        cdef uint16_t val_size = 234


        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)
        self.assertEqual(FIXMsg.group_start(m, 10, 1, 4, [1, 2, 3, 4]), 1)
        cdef FIXGroupRec * g = m.open_group
        val = 123
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 1, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 2, &val, sizeof(int), b'i'), 1)
        self.assertEqual(FIXMsg.group_add_tag(m, 10, 3, &val, sizeof(int), b'i'), 1)
        assert FIXMsg.group_finish(m, 10) == 1
        assert FIXMsg.is_valid(m, ) == 1

        cdef uint16_t *fix_data_el_offsets = <uint16_t *> (<void *> g + sizeof(FIXGroupRec) + g.n_tags * sizeof(uint16_t))

        cdef FIXRec *trec = <FIXRec *> (m.values + fix_data_el_offsets[0])
        trec.tag = 5

        self.assertEqual(FIXMsg.group_get(m, 10, 0, 1, &val_data, &val_size, b'i'), -19)  # ERR_GROUP_CORRUPTED

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)


    def test_groups_resize_and_tags(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef FIXMsgStruct * m_tmp

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 20, 100)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 20

        cdef void* value
        cdef uint16_t value_size
        cdef int t = 1
        cdef uint16_t g_tags[4]

        cdef int i = 123
        cdef double f = 8907.889
        cdef char c = b'V'
        cdef char * s = b'my fancy string, it may be too long!'

        cdef int n_elements = 5
        while t < 100:

            if t % 10 == 0:
                g_tags[0] = t + 1
                g_tags[1] = t + 3
                g_tags[2] = t + 5
                g_tags[3] = t + 7
                has_resize = False
                self.assertEqual(FIXMsg.group_start(m, t, n_elements, 4, g_tags), 1)
                for k in range(n_elements):
                    rc = FIXMsg.group_add_tag(m, t, t + 1, &i, sizeof(int), b'i')

                    if rc == -21:
                        m_tmp = FIXMsg.resize(m, 1, USHRT_MAX-m.header.data_size)
                        assert m_tmp != NULL, f'tags={m.header.tags_count} data={m.header.data_size} lastpos={m.header.last_position}'
                        m = m_tmp
                        rc = FIXMsg.group_add_tag(m, t, t + 1, &i, sizeof(int), b'i')
                        has_resize = True
                    self.assertEqual(rc, 1, f't={t} k={k} has_resize={has_resize}')
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 3, &f, sizeof(double), b'f'), 1)
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 5, &c, sizeof(char), b'c'), 1)
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 7, s, strlen(s)+1, b's'), 1, f'k={k}')
                self.assertEqual(FIXMsg.group_finish(m, t), 1, f't={t} k={k}')
                assert FIXMsg.is_valid(m) == 1
                self.assertEqual(FIXMsg.group_count(m, t), n_elements, f'has_resize={has_resize}')

                for k in range(n_elements):
                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 1, &value, &value_size, b'i'), 1,
                                     f't={t} k={k} n_realocs={m.header.n_reallocs} g_tags={g_tags}')
                    assert (<int *> value)[0] == i
                    assert value_size == sizeof(int)

                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 7, &value, &value_size, b's'), 1, f't={t} k={k} n_realocs={m.header.n_reallocs} g_tags={g_tags}')
                    assert value_size == strlen(s) + 1
                    #cybreakpoint(strcmp((<char *> value), s) != 0)
                    assert strcmp((<char *> value), s) == 0, f't={t}, k={k} n_realocs={m.header.n_reallocs} data_size={m.header.data_size}'

                t += 10
            else:
                FIXMsg.set(m, t, &t, sizeof(int), b'i')
                t += 1
        assert FIXMsg.is_valid(m) == 1
        FIXMsg.destroy(m)

    def test_groups_resize_and_multi_types(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef FIXMsgStruct * m_tmp

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 20, UCHAR_MAX)
        assert m.header.data_size == (sizeof(FIXRec) + sizeof(int)) * 20

        cdef void* value
        cdef uint16_t value_size
        cdef int t = 1
        cdef uint16_t g_tags[4]

        cdef int i = 123
        cdef double f = 8907.889
        cdef char c = b'V'
        cdef char * s = b'my fancy string, it may be too long!'

        cdef int n_elements = 5
        while t < 100:

            if t % 10 == 0:
                g_tags[0] = t + 1
                g_tags[1] = t + 3
                g_tags[2] = t + 5
                g_tags[3] = t + 7

                self.assertEqual(FIXMsg.group_start(m, t, n_elements, 4, g_tags), 1)
                for k in range(n_elements):
                    rc = FIXMsg.group_add_tag(m, t, t + 1, &i, sizeof(int), b'i')
                    has_resize = False
                    if rc == -21:
                        m_tmp = FIXMsg.resize(m, 0, USHRT_MAX-m.header.data_size)
                        assert m_tmp != NULL, f'tags={m.header.tags_count} data={m.header.data_size} lastpos={m.header.last_position}'
                        m = m_tmp
                        rc = FIXMsg.group_add_tag(m, t, t + 1, &i, sizeof(int), b'i')
                        has_resize = True
                    self.assertEqual(rc, 1, f't={t} k={k} has_resize={has_resize}')
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 3, &f, sizeof(double), b'f'), 1)
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 5, &c, sizeof(char), b'c'), 1)
                    self.assertEqual(FIXMsg.group_add_tag(m, t, t + 7, s, strlen(s)+1, b's'), 1, f'k={k}')
                self.assertEqual(FIXMsg.group_finish(m, t), 1, f't={t} k={k}')
                assert FIXMsg.is_valid(m) == 1
                self.assertEqual(FIXMsg.group_count(m, t), n_elements)

                for k in range(n_elements):
                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 1, &value, &value_size, b'i'), 1,
                                     f't={t} k={k} n_realocs={m.header.n_reallocs} g_tags={g_tags}')
                    assert (<int *> value)[0] == i
                    assert value_size == sizeof(int)

                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 7, &value, &value_size, b's'), 1, f't={t} k={k} n_realocs={m.header.n_reallocs} g_tags={g_tags}')
                    assert value_size == strlen(s) + 1
                    #cybreakpoint(strcmp((<char *> value), s) != 0)
                    assert strcmp((<char *> value), s) == 0, f't={t}, k={k} n_realocs={m.header.n_reallocs} data_size={m.header.data_size}'

                t += 10
            else:
                FIXMsg.set(m, t, &t, sizeof(int), b'i')
                t += 1
        # All should be valid
        assert FIXMsg.is_valid(m, ) == 1
        t = 1
        while t < 100:
            if t % 10 == 0:
                for k in range(n_elements):
                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 1, &value, &value_size, b'i'), 1, f't={t} k={k}')
                    assert (<int*>value)[0] == i
                    assert value_size == sizeof(int)

                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 3, &value, &value_size, b'f'), 1)
                    assert (<double *> value)[0] == f
                    assert value_size == sizeof(double)

                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 5, &value, &value_size, b'c'), 1)
                    assert (<char *> value)[0] == c
                    assert value_size == sizeof(char)

                    self.assertEqual(FIXMsg.group_get(m, t, k, t + 7, &value, &value_size, b's'), 1, f'k={k}')
                    assert value_size == strlen(s) + 1
                    assert strcmp((<char *> value), s) == 0, f't={t}, k={k} n_realocs={m.header.n_reallocs} data_size={m.header.data_size}'
                t += 10
            else:
                self.assertEqual(FIXMsg.get(m, t, &value, &value_size, b'i'), 1, f't={t}' )
                assert (<int *> value)[0] == t
                assert value_size == sizeof(int)
                t += 1

        # All should be valid
        assert FIXMsg.is_valid(m, ) == 1
        FIXMsg.destroy(m)

    def test_group_overflow__group_start(self):
        # Exact match no resize
        cdef FIXMsgStruct * m = make_almost_overflowed(<uint16_t>5)
        assert m != NULL
        assert m.header.last_position, 65531  # Real bytes written

        self.assertEqual(FIXMsg.group_start(m, 1000, 2, 3, [101, 102, 103]), -6)
        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_group_overflow__group_add_tag(self):
        cdef FIXMsgStruct * m = make_almost_overflowed(125)
        assert m != NULL
        self.assertEqual(m.header.last_position,  65410)  # Real bytes written

        # 125 bytes remaining to 65535
        # Group start size = 26 bytes
        cdef uint16_t max_records = 110
        self.assertEqual(FIXMsg.group_start(m, max_records + 10,
                                       10,  # N elements
                                       1,  # n - tags
                                       [max_records + 11]),
                         1)

        # 99 bytes remaining for msg size (10 = FIXRec(6) + int(4))
        for i in range(10):
            if i < 8:
                # Good
                self.assertEqual(FIXMsg.group_add_tag(m, max_records + 10, max_records + 11, &i, sizeof(int), b'i'), 1, f'i={i}')
            else:
                # Overflow!
                self.assertEqual(FIXMsg.group_add_tag(m, max_records + 10, max_records + 11, &i, sizeof(int), b'i'), -6, f'i={i}')

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)


    def test_group_overflow__group_finish(self):
        cdef FIXMsgStruct * m = make_almost_overflowed(125)
        assert m != NULL
        self.assertEqual(m.header.last_position, 65410)  # Real bytes written
        cdef uint16_t max_records = 110
        # 125 bytes remaining to 65535
        # Group start size = 30 bytes
        self.assertEqual(FIXMsg.group_start(m, max_records + 10,
                                       8,  # N elements
                                       5,  # n - tags
                                       # Tags must be unique
                                       [max_records + 11,
                                        max_records + 12,
                                        max_records + 13,
                                        max_records + 14,
                                        max_records + 15
                                        ]),
                         1)

        # 95 bytes remaining for msg size (10 = FIXRec(6) + int(4))
        for i in range(8):
            self.assertEqual(FIXMsg.group_add_tag(m, max_records + 10, max_records + 11, &i, sizeof(int), b'i'), 1, f'i={i}')

        # No room for extra FIXRec(6) bytes, overflow
        self.assertEqual(FIXMsg.group_finish(m, max_records + 10), -6)

        self.assertEqual(FIXMsg.group_count(m, max_records + 10), -6) # ERR_DATA_OVERFLOW

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_group_count_errors(self):
        # Exact match no resize
        cdef FIXMsgStruct * m

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 20)

        cdef int i  = 0
        cdef int val

        # Exclude zero tag error and tag 35 error
        max_records = int(USHRT_MAX / (sizeof(FIXRec) + sizeof(int))) - 10

        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(int), b'i'), 1, f'{i}')
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(int), b'i'), -1, f'{i}') # DUPLICATE

        self.assertEqual(FIXMsg.group_count(m, 10), -1)
        self.assertEqual(FIXMsg.group_count(m, USHRT_MAX-10), -6) # ERR_DATA_OVERFLOW

        assert FIXMsg.is_valid(m, ) == 0
        FIXMsg.destroy(m)

    def test_getset_int(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000,20)

        cdef double i = 12.0
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(double), b'i'), 1, f'{i}')

        assert FIXMsg.set_int(m, 12, 123) == 1
        assert FIXMsg.get_int(m, 10) == NULL
        self.assertEqual(FIXMsg.get_last_error(m, ), -20)  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_int(m, 13) == NULL
        assert FIXMsg.get_last_error(m, ) == 0    #ERR_NOT_FOUND
        assert FIXMsg.get_int(m, 12)[0] == 123
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!

        assert FIXMsg.get(m, 12, &value, &size, b'i') == 1
        assert (<int*>value)[0] == 123
        FIXMsg.destroy(m)


    def test_getset_bool(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)

        cdef double i = 12.0
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(double), b'b'), 1, f'{i}')

        assert FIXMsg.set_bool(m, 13, 123) == -20
        assert m.header.tag_errors > 0

        assert FIXMsg.set_bool(m, 12, True) == 1
        assert FIXMsg.get_bool(m, 10) == NULL
        assert FIXMsg.get_last_error(m, ) == -20  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_bool(m, 13) == NULL
        assert FIXMsg.get_last_error(m, ) == 0  #ERR_NOT_FOUND
        assert FIXMsg.get_bool(m, 12)[0] == 1, FIXMsg.get_bool(m, 12)[0]
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!

        assert FIXMsg.get(m, 12, &value, &size, b'b') == 1
        assert (<int8_t *> value)[0] == 1
        FIXMsg.destroy(m)

    def test_getset_char(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)

        cdef double i = 12.0
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(double), b'c'), 1, f'{i}')

        for c in range(-127, 20):
            # Negative chars, and all less than space ascii <20> not allowed
            assert FIXMsg.set_char(m, 13, c) == -20

        # Char #127 (<del>) not allowed
        assert FIXMsg.set_char(m, 13, 127) == -20

        assert FIXMsg.set_char(m, 12, 20) == 1
        assert FIXMsg.set_char(m, 15, 126) == 1

        assert FIXMsg.get_char(m, 10) == NULL
        assert FIXMsg.get_last_error(m, ) == -20  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_char(m, 13) == NULL
        assert FIXMsg.get_last_error(m, ) == 0  #ERR_NOT_FOUND
        assert FIXMsg.get_char(m, 12)[0] == 20
        assert FIXMsg.get_char(m, 15)[0] == 126
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!

        assert FIXMsg.get(m, 12, &value, &size, b'c') == 1
        assert (<char *> value)[0] == 20
        assert FIXMsg.get(m, 15, &value, &size, b'c') == 1
        assert (<char *> value)[0] == 126
        FIXMsg.destroy(m)


    def test_getset_double(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)

        cdef int i = 12
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(int), b'f'), 1, f'{i}')

        assert FIXMsg.set_double(m, 12, 123.456) == 1
        assert FIXMsg.get_double(m, 10) == NULL
        assert FIXMsg.get_last_error(m, ) == -20  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_double(m, 13) == NULL
        assert FIXMsg.get_last_error(m, ) == 0    #ERR_NOT_FOUND
        assert FIXMsg.get_double(m, 12)[0] == 123.456
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!

        assert FIXMsg.get(m, 12, &value, &size, b'f') == 1
        assert (<double*>value)[0] == 123.456
        FIXMsg.destroy(m)

    def test_getset_timestamp(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        cdef long dt_now = datetime_nsnow()

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)

        cdef int i = 12
        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(int), b't'), 1, f'{i}')

        assert FIXMsg.set_utc_timestamp(m, 12, dt_now) == 1
        assert FIXMsg.get_utc_timestamp(m, 10) == NULL
        assert FIXMsg.get_last_error(m, ) == -20  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_utc_timestamp(m, 13) == NULL
        assert FIXMsg.get_last_error(m, ) == 0  #ERR_NOT_FOUND
        assert FIXMsg.get_utc_timestamp(m, 12)[0] == dt_now
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!

        assert FIXMsg.get(m, 12, &value, &size, b't') == 1
        assert (<long *> value)[0] == dt_now
        FIXMsg.destroy(m)


    def test_getset_str(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        cdef void * value
        cdef uint16_t size

        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)

        cdef double i = 0
        cdef char* test_s = b'123132213123'

        # Empty strings are not allowed!
        assert m.header.tag_errors == 0
        self.assertEqual(FIXMsg.set_str(m, 12, b'', 0), -20)
        assert m.header.tag_errors == 1

        self.assertEqual(FIXMsg.set(m, 10, &i, sizeof(double), b's'), -20, f'{i}')
        self.assertEqual(FIXMsg.set(m, 11, &test_s, strlen(test_s), b's'), -20, f'{i}')
        assert m.header.tag_errors == 3

        self.assertEqual(FIXMsg.set_str(m, 13, b'abc', 0), 1)
        assert FIXMsg.set_str(m, 14, b'a', 3) == -20 # ERR_UNEXPECTED_TYPE_SIZE
        assert FIXMsg.set_str(m, 15, test_s, 3) == -20
        assert FIXMsg.set_str(m, 16, b'dfe', 3) == 1

        assert FIXMsg.get_str(m, 10) == NULL
        assert FIXMsg.get_last_error(m, ) == -20  #ERR_UNEXPECTED_TYPE_SIZE   = -20
        assert FIXMsg.get_str(m, 17) == NULL
        assert FIXMsg.get_last_error(m, ) == 0  #ERR_NOT_FOUND
        assert FIXMsg.get_str(m, 12) == NULL
        assert FIXMsg.get_last_error(m, ) == 0  #ERR_NOT_FOUND

        assert FIXMsg.get_str(m, 13) != NULL
        self.assertEqual(FIXMsg.get_last_error(m, ), 1)
        assert FIXMsg.get_str(m, 16) != NULL
        assert strcmp(FIXMsg.get_str(m, 13), b'abc') == 0
        assert strcmp(FIXMsg.get_str(m, 16), b'dfe') == 0

        assert FIXMsg.get(m, 16, &value, &size, b's') == 1
        assert strcmp((<char *> value), b'dfe') == 0
        assert FIXMsg.get_last_error(m, ) == 1  # Success no error!
        FIXMsg.destroy(m)

    def test_get_last_error(self):
        # Exact match no resize
        cdef FIXMsgStruct * m
        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 2000, 10)
        self.assertEqual(FIXMsg.get_last_error_str(m, 1),  b'No error')
        self.assertEqual(FIXMsg.get_last_error_str(m, 0),  b'Not found')
        self.assertEqual(FIXMsg.get_last_error_str(m, -1), b'Duplicated tag')
        self.assertEqual(FIXMsg.get_last_error_str(m, -2), b'Tag type mismatch')
        self.assertEqual(FIXMsg.get_last_error_str(m, -3), b'Value size exceeds 1024 limit')
        self.assertEqual(FIXMsg.get_last_error_str(m, -4), b'FIX(35) tag or type value is not allowed')
        self.assertEqual(FIXMsg.get_last_error_str(m, -5), b'FIX tag=0 is not allowed')
        self.assertEqual(FIXMsg.get_last_error_str(m, -6), b'FIX tag>=65525 or message capacity overflow')
        self.assertEqual(FIXMsg.get_last_error_str(m, -7), b'System memory error when resizing the message')
        self.assertEqual(FIXMsg.get_last_error_str(m, -8), b'You must finish the started group before using other methods')
        self.assertEqual(FIXMsg.get_last_error_str(m, -9), b'Group with zero members are not allowed')
        self.assertEqual(FIXMsg.get_last_error_str(m, -10), b'Group member tag is a duplicate with other tags added to message')
        self.assertEqual(FIXMsg.get_last_error_str(m, -11), b'You must call group_start() before adding group members')
        self.assertEqual(FIXMsg.get_last_error_str(m, -12), b'group_tag must match to the tag of the group_start()')
        self.assertEqual(FIXMsg.get_last_error_str(m, -13), b'Too many tags in the group, max 127 allowed')
        self.assertEqual(FIXMsg.get_last_error_str(m, -14), b'You must always add the first group item with the first tag in the group tag list')
        self.assertEqual(FIXMsg.get_last_error_str(m, -15), b'Group element is out of bounds, given at group_start()')
        self.assertEqual(FIXMsg.get_last_error_str(m, -16), b'Group member `tag` in not in tag list at group_start()')
        self.assertEqual(FIXMsg.get_last_error_str(m, -17), b'Trying to finish group with incomplete elements count added, as expected at group_start()')
        self.assertEqual(FIXMsg.get_last_error_str(m, -18), b'You must add group tags in the same order as tag groups at group_start()')
        self.assertEqual(FIXMsg.get_last_error_str(m, -19), b'Group data is corrupted')
        self.assertEqual(FIXMsg.get_last_error_str(m, -20), b'Tag actual value or size does not match expected type size/value boundaries')
        self.assertEqual(FIXMsg.get_last_error_str(m, -21), b'Message is out of tag/data capacity, you need to call FIXMsg.resize(...) or increase initial capacity')
        self.assertEqual(FIXMsg.get_last_error_str(m, -22), b'Message is read-only')
        self.assertEqual(FIXMsg.get_last_error_str(m, -23), b'unknown error code')

        FIXMsg.destroy(m)

    def test_fix_group_sample(self):
        m = FIXMsg.create(<char> b'@', (sizeof(FIXRec) + sizeof(int)) * 200, 10)
        cdef uint16_t n_elements = 5

        cdef int i = 123
        cdef double f = 8907.889
        cdef char c = b'V'
        cdef char * s = b'my fancy string, it may be too long!'

        assert FIXMsg.group_start(m, 100, n_elements, 4,  [10, 11, 12, 13]) == 1
        for k in range(n_elements):
            # start_tag is mandatory! TAG ORDER MATTERS!
            self.assertEqual(FIXMsg.group_add_tag(m, 100, 10, &i, sizeof(int), b'i'), 1)
            self.assertEqual(FIXMsg.group_add_tag(m, 100, 11, &f, sizeof(double), b'f'), 1)
            # Other tags may be omitted or optional
            #FIXMsg.group_add_tag(m, 100, 12, &c, sizeof(char), b'c')
            self.assertEqual(FIXMsg.group_add_tag(m, 100, 13, s, strlen(s) + 1, b's'), 1)
        self.assertEqual(FIXMsg.group_finish(m, 100), 1)

        assert FIXMsg.group_count(m, 100) == 5

        FIXMsg.destroy(m)
