from uberhf.datafeed.quotes_cache cimport QCRecord
from .fix_orders cimport FIXNewOrderSingle
from libc.stdint cimport uint64_t
from .smart_order_base cimport SmartOrderBase

cdef class OMSAbstract:
    cdef uint64_t _gen_clord_id(self):
        return 0

    cdef QCRecord * quote_get_subscribe(self, bytes smart_order_clord_id, char * v2_ticker, long ticker_id, int ticker_index)  except NULL:
        return NULL

    cdef int gate_send_order_new(self, bytes smart_order_clord_id, FIXNewOrderSingle order):
        return -1
    cdef int gate_send_order_cancel(self, bytes smart_order_clord_id, FIXNewOrderSingle order):
        return -1
    cdef int gate_send_order_replace(self, bytes smart_order_clord_id, FIXNewOrderSingle order, double price, double qty):
        return -1
    cdef int gate_on_execution_report(self, FIXMsgStruct * exec_rep):
        return -1