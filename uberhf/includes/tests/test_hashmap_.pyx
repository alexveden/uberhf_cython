import time
import unittest
import zmq
from libc.string cimport strcpy, memset, strcmp
from uberhf.includes.hashmap cimport HashMap

ctypedef struct Struct1:
    char sender_id[5]
    int value

class CyHashmapTestCase(unittest.TestCase):

    def test_init_strings(self):
        cdef HashMap hm = HashMap(5)
        #hm.count()
        assert hm.get(b'1234') == NULL
        assert hm.set(b'1234') == NULL
        assert <char*>hm.get(b'1234') == b'1234'
        assert hm.set(b'5678') == NULL
        assert hm.count() == 2

        assert <char*>hm.set(b'5678') == b'5678'
        assert <char*>hm.delete(b'1234') == b'1234'
        assert hm.count() == 1
        assert hm.delete(b'00999') == NULL
        hm.clear()
        assert hm.count() == 0



    def test_init_structs(self):
        cdef HashMap hm = HashMap(sizeof(Struct1))
        cdef Struct1 s;
        memset(s.sender_id, 98, 5)
        strcpy(s.sender_id, b'1234')
        s.value = 1
        assert hm.set(&s) == NULL

        strcpy(s.sender_id, b'')
        s.value = 10
        assert strcmp(s.sender_id, b'') == 0
        assert hm.set(&s) == NULL

        strcpy(s.sender_id, b'5678')
        s.value = 2
        assert hm.set(&s) == NULL

        cdef Struct1 * ps = <Struct1*> hm.get(&s)
        assert ps != NULL
        # Make sure that ps is not a referece of s, but a copy
        strcpy(s.sender_id, b'9999')
        s.value = 3
        assert strcmp(ps.sender_id, b'5678') == 0
        assert ps.value == 2

        strcpy(s.sender_id, b'1234')
        s.value = 1
        ps = <Struct1 *> hm.get(b'1234')
        assert ps != NULL
        # Make sure that ps is not a referece of s, but a copy
        assert strcmp(ps.sender_id, b'1234') == 0
        assert ps.value == 1

        # Check if we can directly edit this poinder
        ps.value = 5
        ps = <Struct1 *> hm.get(b'1234')
        assert ps != NULL
        # Make sure that ps is not a referece of s, but a copy
        assert strcmp(ps.sender_id, b'1234') == 0
        assert ps.value == 5

        cdef size_t i = 0
        cdef void * hm_data = NULL
        while hm.iter(&i, &hm_data):
            ps = <Struct1*> hm_data
            if ps.value < 10:
                assert len(ps.sender_id) == 4
            else:
                assert len(ps.sender_id) == 0



