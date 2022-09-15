from typing import Tuple
from libc.stdlib cimport malloc, free
from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsg, FIXMsgStruct
from libc.stdint cimport uint64_t
from libc.math cimport NAN, isnan
from math import isfinite
from uberhf.includes.utils cimport strlcpy
from uberhf.datafeed.quotes_cache cimport QCRecord
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN
from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP,\
                                      FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
                                      FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
                                      FIX_ET_STATUS, FIXNewOrderSingle


cdef class FIXMsgC:
    def __cinit__(self, uint64_t msg_ptr):
        self.m = <FIXMsgStruct*>msg_ptr

    def __dealloc__(self):
        if self.m != NULL:
            FIXMsg.destroy(self.m)
            self.m = NULL

cdef class DummyQuote:
    cdef QCRecord * q

    def __cinit__(self, v2_ticker, bid, ask, last):
        assert len(v2_ticker) < V2_TICKER_MAX_LEN
        assert isinstance(v2_ticker, bytes), 'bytes expected'
        cdef QCRecord *q = <QCRecord*>malloc(sizeof(QCRecord))

        strlcpy(q.v2_ticker, v2_ticker, V2_TICKER_MAX_LEN)

        q.ticker_index = -1
        q.instrument_id = abs(hash(v2_ticker))
        q.quote.bid = bid
        q.quote.ask = ask
        q.quote.last = last

        self.q = q

    def __dealloc__(self):
        if self.q != NULL:
            free(self.q)
            self.q = NULL


cdef class FIXTester(OMSAbstract):
    def __cinit__(self):
        self.data2smart = {}
        self.smart2orders = {}
        self.orders2smart = {}
        self.data_cache = {}
        self._clord_id_cnt = 0

    def set_prices(self, *args: Tuple[str, float, float, float]):
        for (v2_ticker, bid, ask, last) in args:
            if isinstance(v2_ticker, str):
                v2_ticker = v2_ticker.encode()
            elif not isinstance(v2_ticker, bytes):
                raise ValueError("v2_ticker must be a str or bytes")

            q = DummyQuote(v2_ticker, bid, ask, last)
            assert v2_ticker not in self.data_cache, 'All v2_tickers must be unique!'
            self.data_cache[v2_ticker] = q

    cdef QCRecord * quote_get_subscribe(self, bytes smart_order_clord_id, char * v2_ticker, long ticker_id, int ticker_index) except NULL:
        cdef DummyQuote qdummy = <DummyQuote>self.data_cache.get(v2_ticker)
        if qdummy is None:
            raise ValueError(f'{v2_ticker} not in cache')
        data_subs = self.data2smart.setdefault(v2_ticker, {})
        data_subs.setdefault(smart_order_clord_id, {})
        return qdummy.q

    cdef int gate_send_order_new(self, bytes smart_order_clord_id, FIXNewOrderSingle order):
        return -1
    cdef int gate_send_order_cancel(self, bytes smart_order_clord_id, FIXNewOrderSingle order):
        return -1
    cdef int gate_send_order_replace(self, bytes smart_order_clord_id, FIXNewOrderSingle order, double price, double qty):
        return -1
    cdef int gate_on_execution_report(self, FIXMsgStruct * exec_rep):
        return -1

    cdef int order_register_single(self, FIXNewOrderSingle order):
        cdef uint64_t clord_id = self._gen_clord_id()

        assert order.clord_id == 0
        rc = order.register(order.msg, clord_id, FIX_OS_CREA)
        assert rc == 1, f'order.register(order.msg, clord_id, FIX_OS_CREA) failed, reason: {rc}'
        assert order.clord_id == clord_id
        return 1

    cdef int order_register_cxlrep(self, FIXNewOrderSingle order, FIXMsgStruct * m):
        assert m.header.msg_type == b'F' or m.header.msg_type == b'G'
        cdef uint64_t new_clord_id = self._gen_clord_id()
        cdef uint64_t * orig_clord_id = FIXMsg.get_uint64(m, 41)
        assert orig_clord_id != NULL, f'Tag 41 not found?, rc: {FIXMsg.get_last_error(m)}'

        assert order.clord_id != 0
        assert order.clord_id == orig_clord_id[0]
        if m.header.msg_type == b'F':
            rc = order.register(m, new_clord_id, FIX_OS_PCXL)
        else:
            assert m.header.msg_type == b'G'
            rc = order.register(m, new_clord_id, FIX_OS_PREP)

        assert rc == 1, f'o.register(clord_id, orig_clord_id) failed, reason: {rc}'
        assert order.clord_id == new_clord_id
        assert order.orig_clord_id == orig_clord_id[0]
        return 1

    cdef uint64_t _gen_clord_id(self):
        self._clord_id_cnt += 1
        return self._clord_id_cnt

    cdef FIXMsgC fix_cxl_request(self, FIXNewOrderSingle order):
        cdef FIXMsgStruct * m = order.cancel_req()
        assert m != NULL, f'order.last_fix_error={order.last_fix_error}'
        assert self.order_register_cxlrep(order, m) == 1
        return FIXMsgC(<uint64_t>m)

    cdef FIXMsgC fix_rep_request(self, FIXNewOrderSingle order, double price = NAN, double qty = NAN):
        cdef FIXMsgStruct * m = order.replace_req(price, qty)
        assert m != NULL, f'order.last_fix_error={order.last_fix_error}'
        assert self.order_register_cxlrep(order, m) == 1
        return FIXMsgC(<uint64_t>m)

    cdef FIXMsgC fix_cxlrep_reject_msg(self,
                                       FIXMsgC cancel_msg,
                                       char ord_status,
                                       ):
        cdef FIXMsgStruct * cxl_req = cancel_msg.m
        assert cxl_req.header.msg_type == b'F' or cxl_req.header.msg_type == b'G', chr(cxl_req.header.msg_type)
        cdef uint64_t * clord_id = FIXMsg.get_uint64(cxl_req, 11)
        assert clord_id != NULL, FIXMsg.get_last_error_str(FIXMsg.get_last_error(cxl_req))
        cdef uint64_t * orig_clord_id = FIXMsg.get_uint64(cxl_req, 41)
        assert orig_clord_id != NULL, FIXMsg.get_last_error_str(FIXMsg.get_last_error(cxl_req))


        cdef FIXMsgStruct * m = FIXMsg.create(b'9', 300, 30)
        # OrderID is zero (so far)
        assert FIXMsg.set_uint64(m, 37, 0) == 1
        assert FIXMsg.set_uint64(m, 11, clord_id[0]) == 1
        assert FIXMsg.set_uint64(m, 41, orig_clord_id[0]) == 1
        assert FIXMsg.set_char(m, 39, ord_status) == 1
        """
        CxlRejResponseTo <434> field – FIX 4.4 – FIX Dictionary
        Description
        Identifies the type of request that a Cancel Reject <9> is in response to.
        
        1 = Order Cancel Request <F>
        2 = Order Cancel/Replace Request <G>
        """
        if cxl_req.header.msg_type == b'F':
            assert FIXMsg.set_char(m, 434, b'1') == 1
        elif cxl_req.header.msg_type == b'G':
            assert FIXMsg.set_char(m, 434, b'2') == 1
        else:
            assert False

        assert FIXMsg.is_valid(m) == 1

        msg_c = FIXMsgC(<uint64_t>m)
        return msg_c

    cdef FIXMsgC fix_exec_report_msg(self,
                             FIXNewOrderSingle order,
                             uint64_t clord_id,
                             char exec_type,
                             char ord_status,
                             double cum_qty = NAN,
                             double leaves_qty = NAN,
                             double last_qty = NAN,
                             double price = NAN,
                             double order_qty = NAN,
                             uint64_t orig_clord_id = 0,
                            ):
        assert order.clord_id > 0, 'Unregistered order!'

        cdef FIXMsgStruct * m = FIXMsg.create(b'8', 300, 30)
        assert clord_id > 0
        assert FIXMsg.set_uint64(m, 11,  clord_id) == 1

        if orig_clord_id > 0:
            # TAG 41 Orig Clord ID - must be set explicitly
            assert FIXMsg.set_uint64(m, 41, orig_clord_id) == 1

        # TAG 150 Exec type
        assert FIXMsg.set_char(m, 150, exec_type) == 1

        # TAG 39 Ord status
        assert FIXMsg.set_char(m, 39, ord_status) == 1

        # TAG 38 Qty
        if isnan(order_qty):
            order_qty = order.qty
        else:
            assert exec_type == b'5', f'Only applicable to exec_type=5 (replace)'
            assert order_qty > 0
        assert FIXMsg.set_double(m, 38, order_qty) == 1

        # TAG 38 CumQty
        if isnan(cum_qty):
            cum_qty = order.cum_qty
        else:
            assert cum_qty <= order.qty
            assert cum_qty >= 0
        assert FIXMsg.set_double(m, 14, cum_qty) == 1

        # TAG 38 LeavesQty
        if isnan(leaves_qty):
            leaves_qty = order.leaves_qty
        else:
            assert leaves_qty >= 0
            assert leaves_qty <= order_qty
        assert FIXMsg.set_double(m, 151, leaves_qty) == 1
        assert cum_qty + leaves_qty <= order_qty, f'cum_qty[{cum_qty}] + leaves_qty[{leaves_qty}] <= order_qty[{order_qty}]'

        if not isnan(last_qty):
            assert not isnan(leaves_qty), f'Must also set leaves_qty, when trade'
            assert not isnan(cum_qty), f'Must also set cum_qty, when trade'
            assert exec_type == b'F', 'Only applicable to exec_type=F (trade)'
            assert last_qty > 0
            assert FIXMsg.set_double(m, 32, last_qty) == 1

            assert round(last_qty-(cum_qty-order.cum_qty), 3) == 0, f'Probably incorrect Trade qty'
        else:
            assert exec_type != b'F', 'You must set last_qty when exec_type=F (trade)'


        if not isnan(price):
            assert exec_type == b'5', f'Only applicable to exec_type=5 (replace)'
            assert FIXMsg.set_double(m, 44, price) == 1

        if exec_type == FIX_ET_PCXL and ord_status == FIX_OS_PCXL:
            assert order.orig_clord_id > 0
            assert order.clord_id > 0
            assert order.clord_id > order.orig_clord_id
            assert order.cum_qty == cum_qty
            assert order.leaves_qty == leaves_qty

        if ord_status == FIX_OS_FILL or ord_status == FIX_OS_CXL or ord_status == FIX_OS_REJ or ord_status == FIX_OS_EXP:
            assert leaves_qty == 0, f'New order report is finished, but LeavesQty != 0'

        assert FIXMsg.is_valid(m) == 1

        msg_c = FIXMsgC(<uint64_t>m)
        return msg_c






