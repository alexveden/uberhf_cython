from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsgStruct, FIXMsg
from libc.stdint cimport uint64_t, uint16_t, int8_t, uint8_t
from .fix_orders cimport FIXNewOrderSingle
from uberhf.datafeed.quotes_cache cimport QCRecord

from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP, \
    FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
    FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
    FIX_ET_STATUS, FIX_OS_PFILL, FIXNewOrderSingle

cdef class OrdPosition:
    cdef readonly bytes v2_ticker
    cdef readonly long exchange_ticker_id
    cdef readonly int ticker_index
    cdef readonly double qty
    cdef readonly double value
    cdef readonly double fixed_point_value
    cdef readonly double realized_pnl
    cdef readonly double costs


cdef class SmartOrderBase:
    cdef OMSAbstract oms
    cdef FIXMsgStruct * smart_msg
    cdef bytes smart_clord_id
    cdef dict orders  # type: Dict[uint64_t, SingleOrder]
    cdef dict position  # type: Dict[v2_ticker, OrdPosition]
    cdef char status
    cdef double qty
    cdef double qty_fill

    cdef int smart_order_new(self)
    cdef int smart_order_load(self)
    cdef int smart_order_invalidate(self)
    cdef int smart_order_finish(self)

    cdef QCRecord * quote(self, char * v2_ticker)
    cdef send(self, FIXNewOrderSingle order)
    cdef cancel(self, FIXNewOrderSingle order)
    cdef replace(self, FIXNewOrderSingle order, double price, double qty)
    cdef finalize(self, FIXNewOrderSingle order)

    # cdef FIXNewOrderSingle get(self, bytes smart_key)
    # cdef bint has(self, smart_key: str)
    # cdef items(self)
    # cdef int count(self)

    cdef on_quote(self, QCRecord * q)
    cdef on_order_quote(self, FIXNewOrderSingle o)
    cdef on_order_status(self, FIXNewOrderSingle o)
    cdef on_order_trade(self, FIXNewOrderSingle o)
    cdef _on_oms_trade_report(self, QCRecord *q, double trade_qty, double trade_price)


