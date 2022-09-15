import time
import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, TIMEDELTA_MILLI, timedelta_ns
from libc.stdint cimport uint64_t, uint16_t
from libc.string cimport memcmp, strlen, strcmp, memcpy, memset
from libc.stdlib cimport malloc, free
from uberhf.prototols.messages cimport *
from uberhf.orders.fix_orders import FIXNewOrderSingle
from libc.limits cimport USHRT_MAX
from uberhf.orders.fix_msg cimport FIXMsgStruct, FIXMsg
from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP,\
                                      FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
                                      FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
                                      FIX_ET_STATUS, FIX_OS_PFILL, FIXNewOrderSingle
from uberhf.datafeed.quotes_cache cimport QCRecord
from uberhf.orders.smart_order_base cimport SmartOrderBase
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN
from uberhf.orders.fix_tester import FIXTester, FIXMsgC
from uberhf.orders.fix_tester cimport FIXTester, FIXMsgC
from libc.math cimport NAN

assert V2_TICKER_MAX_LEN == 40
cdef QCRecord global_q
#                           b'OC.RU.<F.RTS.H21>.202123@12934'
assert strlcpy(global_q.v2_ticker, b'012345678901234567890123456789012345678', V2_TICKER_MAX_LEN) == 39
global_q.ticker_index = 10
global_q.instrument_id = 123

cdef class SmartOrderLimit(SmartOrderBase):
    @staticmethod
    cdef FIXMsgStruct * smart_order_construct_msg(char * clord_id, QCRecord * q, double price, double qty):
        cdef FIXMsgStruct * m = FIXMsg.create(b'@', 200, 20)
        assert FIXMsg.set_str(m, 11, clord_id, 0) > 0    # ClOrd ID
        assert FIXMsg.set_int(m, 1, 100100100) > 0    # Account

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        assert FIXMsg.set_int(m, 22, q.ticker_index) > 0
        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        assert FIXMsg.set_uint64(m, 48, q.instrument_id) > 0
        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        assert FIXMsg.set_str(m, 55, q.v2_ticker, 0) > 0

        # Tag 38: Order Qty
        assert FIXMsg.set_double(m, 38, qty) > 0

        # Tag 44: Order Price
        assert FIXMsg.set_double(m, 44, price) > 0
        assert FIXMsg.is_valid(m) == 1

        return m

    cdef int smart_order_new(self):
        pass

class CySmartOrderBaseTestCase(unittest.TestCase):
    def test_init_smart_order(self):
        cdef FIXTester ft = FIXTester()
        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)

        # Pointers have to be different, because message was copied!
        assert smo.smart_msg != smart_msg.m
        assert FIXMsg.is_valid(smo.smart_msg)
        assert smo.status == FIX_OS_CREA
        assert smo.orders == {}
        assert smo.position == {}
        assert smo.oms == ft
        assert smo.smart_clord_id == b'123'

    def test_getting_quote(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        assert q.v2_ticker == b'RU.F.RTS'
        assert q.quote.bid == 99
        assert q.quote.ask == 101
        assert q.quote.last == 100



