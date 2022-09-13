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
from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP,\
                                      FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
                                      FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
                                      FIX_ET_STATUS, FIX_OS_PFILL, FIXNewOrderSingle
from uberhf.datafeed.quotes_cache cimport QCRecord
from uberhf.orders.fix_msg cimport FIXMsg, FIXMsgStruct
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN
from uberhf.orders.fix_tester cimport FIXTester, FIXMsgC

assert V2_TICKER_MAX_LEN == 40
cdef QCRecord q
#                           b'OC.RU.<F.RTS.H21>.202123@12934'
assert strlcpy(q.v2_ticker, b'012345678901234567890123456789012345678', V2_TICKER_MAX_LEN) == 39
q.ticker_index = 10
q.instrument_id = 123

class CyFIXOrdersTestCase(unittest.TestCase):
    def test_init_order_single_default_short(self):
        assert V2_TICKER_MAX_LEN == 40
        cdef QCRecord q
        #                           b'OC.RU.<F.RTS.H21>.202123@12934'
        assert strlcpy(q.v2_ticker, b'012345678901234567890123456789012345678', V2_TICKER_MAX_LEN) == 39
        q.ticker_index = 10
        q.instrument_id = 123

        o = FIXNewOrderSingle.create(&q, 1010, 100, -20)
        assert isinstance(o, FIXNewOrderSingle)
        assert o.q == &q
        assert o.status == FIX_OS_CREA
        assert o.price == 100
        assert o.qty == 20
        assert o.leaves_qty == 0
        assert o.cum_qty == 0
        assert o.clord_id == 0
        assert o.orig_clord_id == 0
        assert o.side == -1
        assert o.target_price == 100

        assert o.msg != NULL
        cdef FIXMsgStruct * m = o.msg

        self.assertEqual(m.header.msg_type, <char>b'D')

        self.assertEqual(m.header.tags_count, 11)
        self.assertEqual(m.header.data_size - m.header.last_position, 0)

        # Account
        assert FIXMsg.get_int(m, 1) != NULL
        assert FIXMsg.get_int(m, 1)[0] == 1010

        # Tag 11: ClOrdID
        assert FIXMsg.get_uint64(m, 11) != NULL
        assert FIXMsg.get_uint64(m, 11)[0] == 0

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        assert FIXMsg.get_int(m, 22) != NULL
        assert FIXMsg.get_int(m, 22)[0] == 10

        # Tag 38: Order Qty
        assert FIXMsg.get_double(m, 38) != NULL
        assert FIXMsg.get_double(m, 38)[0] == 20

        # Tag 40: Order Type
        assert FIXMsg.get_char(m, 40) != NULL
        assert FIXMsg.get_char(m, 40)[0] == b'2'  # Limit order

        # Tag 44: Order Price
        assert FIXMsg.get_double(m, 44) != NULL
        assert FIXMsg.get_double(m, 44)[0] == 100

        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        assert FIXMsg.get_uint64(m, 48) != NULL
        assert FIXMsg.get_uint64(m, 48)[0] == 123

        # Tag 54: Side
        assert FIXMsg.get_char(m, 54) != NULL
        assert FIXMsg.get_char(m, 54)[0] == b'2'  # sell

        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        assert FIXMsg.get_str(m, 55) != NULL
        assert strcmp(FIXMsg.get_str(m, 55), q.v2_ticker) == 0

        # Tag 59: Time in force
        assert FIXMsg.get_char(m, 59) != NULL
        assert FIXMsg.get_char(m, 59)[0] == b'0'  # sell

        # Tag 60: Transact time
        assert FIXMsg.get_utc_timestamp(m, 60) != NULL
        assert timedelta_ns(datetime_nsnow(), FIXMsg.get_utc_timestamp(m, 60)[0], TIMEDELTA_MILLI) < 20

        # Overall message is valid!
        assert FIXMsg.is_valid(m) == 1

    def test_init_order_single_long(self):
        assert V2_TICKER_MAX_LEN == 40
        cdef QCRecord q
        #                           b'OC.RU.<F.RTS.H21>.202123@12934'
        assert strlcpy(q.v2_ticker, b'012345678901234567890123456789012345678', V2_TICKER_MAX_LEN) == 39
        q.ticker_index = 10
        q.instrument_id = 123

        o = FIXNewOrderSingle.create(&q, 1010, 200, 20, target_price=220, order_type=b'm', time_in_force=b'1')
        assert isinstance(o, FIXNewOrderSingle)
        assert o.q == &q
        assert o.status == FIX_OS_CREA
        assert o.price == 200
        assert o.qty == 20
        assert o.leaves_qty == 0
        assert o.cum_qty == 0
        assert o.clord_id == 0
        assert o.orig_clord_id == 0
        assert o.side == 1
        assert o.target_price == 220

        assert o.msg != NULL
        cdef FIXMsgStruct * m = o.msg

        self.assertEqual(m.header.msg_type, <char>b'D')

        self.assertEqual(m.header.tags_count, 11)
        self.assertEqual(m.header.data_size - m.header.last_position, 0)

        # Account
        assert FIXMsg.get_int(m, 1) != NULL
        assert FIXMsg.get_int(m, 1)[0] == 1010

        # Tag 11: ClOrdID
        assert FIXMsg.get_uint64(m, 11) != NULL
        assert FIXMsg.get_uint64(m, 11)[0] == 0

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        assert FIXMsg.get_int(m, 22) != NULL
        assert FIXMsg.get_int(m, 22)[0] == 10

        # Tag 38: Order Qty
        assert FIXMsg.get_double(m, 38) != NULL
        assert FIXMsg.get_double(m, 38)[0] == 20

        # Tag 40: Order Type
        assert FIXMsg.get_char(m, 40) != NULL
        assert FIXMsg.get_char(m, 40)[0] == b'm'  # Limit order

        # Tag 44: Order Price
        assert FIXMsg.get_double(m, 44) != NULL
        assert FIXMsg.get_double(m, 44)[0] == 200

        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        assert FIXMsg.get_uint64(m, 48) != NULL
        assert FIXMsg.get_uint64(m, 48)[0] == 123

        # Tag 54: Side
        assert FIXMsg.get_char(m, 54) != NULL
        assert FIXMsg.get_char(m, 54)[0] == b'1'  # buy

        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        assert FIXMsg.get_str(m, 55) != NULL
        assert strcmp(FIXMsg.get_str(m, 55), q.v2_ticker) == 0

        # Tag 59: Time in force
        assert FIXMsg.get_char(m, 59) != NULL
        assert FIXMsg.get_char(m, 59)[0] == b'1'

        # Tag 60: Transact time
        assert FIXMsg.get_utc_timestamp(m, 60) != NULL
        assert timedelta_ns(datetime_nsnow(), FIXMsg.get_utc_timestamp(m, 60)[0], TIMEDELTA_MILLI) < 20

        # Overall message is valid!
        assert FIXMsg.is_valid(m) == 1

    def test_simple_execution_report_state_created__2__pending_new(self):
        o = FIXNewOrderSingle.create(&q, 1010, 200, 20)
        assert o.status == FIX_OS_CREA, f'o.status={chr(o.status)}'

        ft = FIXTester()
        assert ft.order_register_single(o) == 1
        assert o.status == FIX_OS_CREA, f'o.status={chr(o.status)}'

        cdef FIXMsgC msg = ft.fix_exec_report_msg(o,
                                                  o.clord_id,
                                                  FIX_ET_PNEW,
                                                  FIX_OS_PNEW)
        assert o.process_execution_report(msg.m) == 1
        assert o.status == FIX_OS_PNEW, f'o.status={chr(o.status)}'

    def test_simple_execution_report_state_created__2__rejected(self):
        o = FIXNewOrderSingle.create(&q, 1010, 200, 20)
        assert o.status == FIX_OS_CREA, f'o.status={chr(o.status)}'

        ft = FIXTester()
        assert ft.order_register_single(o) == 1
        assert o.status == FIX_OS_CREA, f'o.status={chr(o.status)}'

        cdef FIXMsgC msg = ft.fix_exec_report_msg(o,
                                                  o.clord_id,
                                                  FIX_ET_REJ,
                                                  FIX_OS_REJ)
        assert o.process_execution_report(msg.m) == 1
        assert o.status == FIX_OS_REJ, f'o.status={chr(o.status)}'

    def test_state_transition__created__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_NEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_FILL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_DFD) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_CXL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_STP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_REJ) == FIX_OS_REJ
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == FIX_OS_PNEW
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_CALC) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_EXP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_CREA, b'8', FIX_ET_TRADE, FIX_OS_PREP) == -23

    def test_state_transition__pendingnew__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_NEW) == FIX_OS_NEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_DFD) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_STP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_REJ) == FIX_OS_REJ
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == FIX_OS_SUSP
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_CALC) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_EXP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PNEW, b'8', FIX_ET_TRADE, FIX_OS_PREP) == -23

    def test_state_transition__new__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_DFD) == FIX_OS_DFD
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == FIX_OS_PCXL
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_STP) == FIX_OS_STP
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_REJ) == FIX_OS_REJ
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == FIX_OS_SUSP
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_CALC) == FIX_OS_CALC
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_EXP) == FIX_OS_EXP
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_NEW, b'8', FIX_ET_TRADE, FIX_OS_PREP) == FIX_OS_PREP


    def test_state_transition__rejected__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CREA) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__rejected__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CREA) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_REJ, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__filled__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_CREA) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_FILL, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__expired__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_CREA) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_EXP, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__canceled__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_CREA) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_CXL, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__suspended__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_NEW) == FIX_OS_NEW
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_FILL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_DFD) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_STP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_REJ) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_CALC) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_EXP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_SUSP, b'8', FIX_ET_TRADE, FIX_OS_PREP) == -23

    def test_state_transition__partiallyfilled__execution_report(self):
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_NEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_DFD) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == FIX_OS_PCXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_STP) == FIX_OS_STP
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_REJ) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == FIX_OS_SUSP
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_CALC) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_EXP) == FIX_OS_EXP
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PFILL, b'8', FIX_ET_TRADE, FIX_OS_PREP) == FIX_OS_PREP

    def test_state_transition__pendingcancel__execution_report(self):
        # Executin report doesn't not have any effect of pending cancelled state
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

        # But cancel reject request does!
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_NEW) == FIX_OS_NEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_DFD) == FIX_OS_DFD
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_PCXL) == FIX_OS_PCXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_STP) == FIX_OS_STP
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_REJ) == FIX_OS_REJ
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_SUSP) == FIX_OS_SUSP
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_PNEW) == FIX_OS_PNEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_CALC) == FIX_OS_CALC
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_EXP) == FIX_OS_EXP
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PCXL, b'9', 0, FIX_OS_PREP) == FIX_OS_PREP

    def test_state_transition__pendingreplce__execution_report(self):
        # Executin report doesn't not have any effect of pending cancelled state
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_NEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_PFILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_FILL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_DFD) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_CXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_PCXL) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_STP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_REJ) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_SUSP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_PNEW) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_CALC) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_EXP) == 0
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_TRADE, FIX_OS_PREP) == 0

    def test_state_transition__pendingreplce__execution_report_exectype_replace(self):
        # Executin report doesn't not have any effect of pending cancelled state
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_NEW) == FIX_OS_NEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_DFD) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_PCXL) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_STP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_REJ) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_SUSP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_PNEW) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_CALC) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_EXP) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'8', FIX_ET_REP, FIX_OS_PREP) == -23

    def test_state_transition__pendingreplce__ord_reject(self):
        # But cancel reject request does!
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_CREA) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_NEW) == FIX_OS_NEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_PFILL) == FIX_OS_PFILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_FILL) == FIX_OS_FILL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_DFD) == FIX_OS_DFD
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_CXL) == FIX_OS_CXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_PCXL) == FIX_OS_PCXL
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_STP) == FIX_OS_STP
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_REJ) == FIX_OS_REJ
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_SUSP) == FIX_OS_SUSP
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_PNEW) == FIX_OS_PNEW
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_CALC) == FIX_OS_CALC
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_EXP) == FIX_OS_EXP
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_ACCPT) == -23
        assert FIXNewOrderSingle.change_status(FIX_OS_PREP, b'9', 0, FIX_OS_PREP) == FIX_OS_PREP