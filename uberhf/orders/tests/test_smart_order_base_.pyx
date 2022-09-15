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
        cdef QCRecord * q = self.quote(FIXMsg.get_str(self.smart_msg, 55))
        assert q != NULL

        cdef FIXNewOrderSingle o_long = FIXNewOrderSingle.create('test_long', q, 1010, 200, 1, qty=10)
        cdef FIXNewOrderSingle o_short = FIXNewOrderSingle.create('test_short', q, 1010, 210, -1, qty=10)
        assert self.send(o_long) > 0
        assert self.send(o_short) > 0


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

    def test_send(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)
        assert ord.clord_id == 1
        assert b'123' in ft.smart2orders
        assert 1 in ft.smart2orders[b'123']
        assert smo.orders['test'] == ord

        assert ord.status == FIX_OS_CREA
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 0
        assert ord.qty == 10

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        assert ord.status == FIX_OS_PNEW
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 0
        assert ord.qty == 10

        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)
        assert ord.status == FIX_OS_NEW
        assert ord.leaves_qty == 10
        assert ord.cum_qty == 0
        assert ord.qty == 10

        ft.sim_trade('test', 1)
        assert ord.status == FIX_OS_PFILL
        assert ord.leaves_qty == 9
        assert ord.cum_qty == 1
        assert ord.qty == 10

        ft.sim_trade('test', 9)
        assert ord.status == FIX_OS_FILL
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 10
        assert ord.qty == 10

        assert ord.is_finished() == 1

    def test_cancel(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)
        ft.sim_actions_settle(force=True)
        self.assertEqual(smo.cancel(ord), 1)
        assert ord.status == FIX_OS_PCXL

        ft.sim_trade('test', 1)
        assert ord.status == FIX_OS_PCXL
        assert ord.leaves_qty == 9
        assert ord.cum_qty == 1
        assert ord.qty == 10

        ft.sim_cancel('test')
        assert ord.status == FIX_OS_CXL
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 1
        assert ord.qty == 10

        assert ord.is_finished() == 1

    def test_cancel_filled_before(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)
        ft.sim_actions_settle(force=True)
        self.assertEqual(smo.cancel(ord), 1)
        assert ord.status == FIX_OS_PCXL
        ft.sim_actions_assert('test', ft.ACT_CANCEL)
        ft.sim_actions_settle()

        ft.sim_trade('test', 1)
        assert ord.status == FIX_OS_PCXL
        assert ord.leaves_qty == 9
        assert ord.cum_qty == 1
        assert ord.qty == 10

        ft.sim_trade('test', 9)
        assert ord.status == FIX_OS_PCXL
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 10
        assert ord.qty == 10

        ft.sim_state('test', None, FIX_OS_FILL)
        assert ord.status == FIX_OS_FILL

        assert ord.is_finished() == 1


    def test_replace(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)
        ft.sim_actions_settle(True)
        self.assertEqual(smo.replace(ord, 300, 20), 1)
        ft.sim_actions_assert('test', ft.ACT_REPLACE, price=300, qty=20)
        ft.sim_actions_settle()
        assert ord.status == FIX_OS_PREP

        ft.sim_trade('test', 2)
        assert ord.status == FIX_OS_PREP
        assert ord.leaves_qty == 8
        assert ord.cum_qty == 2
        assert ord.qty == 10

        ft.sim_replace('test', 300, 20)
        assert ord.status == FIX_OS_PFILL
        assert ord.leaves_qty == 18
        assert ord.cum_qty == 2
        assert ord.qty == 20
        assert ord.price == 300

        self.assertEqual(smo.replace(ord, 300, 2), 1)
        ft.sim_actions_assert('test', ft.ACT_REPLACE, price=300, qty=2)
        ft.sim_actions_settle()
        ft.sim_replace('test', 300, 2)
        assert ord.status == FIX_OS_FILL
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 2
        assert ord.qty == 2
        assert ord.price == 300


    def test_replace__qty_decrease_less_than_filled(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)
        ft.sim_actions_settle(force=True)
        self.assertEqual(smo.replace(ord, 300, 20), 1)
        assert ord.status == FIX_OS_PREP
        ft.sim_actions_assert('test', ft.ACT_REPLACE, price=300, qty=20)
        ft.sim_actions_settle()

        ft.sim_trade('test', 2)
        assert ord.status == FIX_OS_PREP
        assert ord.leaves_qty == 8
        assert ord.cum_qty == 2
        assert ord.qty == 10

        ft.sim_replace('test', 300, 20)
        assert ord.status == FIX_OS_PFILL
        assert ord.leaves_qty == 18
        assert ord.cum_qty == 2
        assert ord.qty == 20
        assert ord.price == 300

        self.assertEqual(smo.replace(ord, 300, 1), 1)
        ft.sim_actions_assert('test', ft.ACT_REPLACE, price=300, qty=1)
        ft.sim_actions_settle()

        ft.sim_replace('test', 300, 2)
        assert ord.status == FIX_OS_FILL
        assert ord.leaves_qty == 0
        assert ord.cum_qty == 2
        assert ord.qty == 2
        assert ord.price == 300

    def test_quote_change(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q.quote.bid == 99
        assert q.quote.ask == 101
        assert q.quote.last == 100

        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10)

        # IMPORTANT: changing QCRecord * q in place!
        ft.sim_quote('RU.F.RTS', 109, 111, 110)
        assert ord.q.quote.bid == 109
        assert ord.q.quote.ask == 111
        assert ord.q.quote.last == 110

    def test_send_action_assert(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, -1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)
        assert ord.clord_id == 1
        assert b'123' in ft.smart2orders
        assert 1 in ft.smart2orders[b'123']
        assert smo.orders['test'] == ord

        assert len(ft.actions) == 1
        ft.sim_actions_assert('test', ft.ACT_SEND, 'RU.F.RTS', price=200, qty=10, side=-1)
        assert len(ft.actions) == 0
        ft.sim_actions_settle()


    def test_cancel_action_assert(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', &global_q, 100, -10))

        cdef SmartOrderBase smo = SmartOrderBase(ft, <uint64_t>smart_msg.m)
        cdef QCRecord * q = smo.quote('RU.F.RTS')
        assert q != NULL
        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create('test', q, 1010, 200, -1, qty=10)
        assert ord.clord_id == 0
        self.assertEqual(smo.send(ord), 1)
        assert ord.clord_id == 1
        assert b'123' in ft.smart2orders
        assert 1 in ft.smart2orders[b'123']
        assert smo.orders['test'] == ord

        assert len(ft.actions) == 1
        ft.sim_actions_assert('test', ft.ACT_SEND, 'RU.F.RTS', price=200, qty=10, side=-1)
        assert len(ft.actions) == 0
        ft.sim_actions_settle()

        ft.sim_state('test', FIX_OS_PNEW, FIX_OS_PNEW)
        ft.sim_state('test', FIX_OS_NEW, FIX_OS_NEW)

        ft.sim_actions_settle()
        self.assertEqual(smo.cancel(ord), 1)
        assert ord.status == FIX_OS_PCXL
        ft.sim_actions_assert('test', ft.ACT_CANCEL)
        ft.sim_actions_settle()



    def test_smart_order_create_new(self):
        cdef FIXTester ft = FIXTester()
        ft.set_prices(('RU.F.RTS', 99, 101, 100))
        cdef QCRecord * q = ft.quote('RU.F.RTS')

        # Using FIXMsgC just for cleanup(__dealloc__) purposes
        cdef FIXMsgC smart_msg = FIXMsgC(<uint64_t>SmartOrderLimit.smart_order_construct_msg(b'123', q, 100, -10))

        cdef SmartOrderLimit smo = SmartOrderLimit(ft, <uint64_t>smart_msg.m)
        smo.smart_order_new()
        ft.sim_actions_assert_count(2)
        ft.sim_actions_assert('test_long', ft.ACT_SEND, v2_ticker='RU.F.RTS', price=200, qty=10, side=1)
        ft.sim_actions_assert('test_short', ft.ACT_SEND, v2_ticker='RU.F.RTS', price=210, qty=10, side=-1)
        ft.sim_actions_settle()


