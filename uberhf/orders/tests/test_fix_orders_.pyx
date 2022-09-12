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
from uberhf.orders.fix_orders import FIXNewOrderSingle
from libc.limits cimport USHRT_MAX


class CyFIXOrdersTestCase(unittest.TestCase):
    def test_init_order_single(self):
        o = FIXNewOrderSingle(b'123', b'acc', None, 100, -1)
        cdef bytes clord_id = o.clord_id

        self.assertEqual(b'abc1', clord_id)
