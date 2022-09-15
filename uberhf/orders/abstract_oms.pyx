from uberhf.datafeed.quotes_cache cimport QCRecord
from .fix_orders cimport FIXNewOrderSingle
from libc.stdint cimport uint64_t
from .smart_order_base cimport SmartOrderBase

cdef class OMSAbstract:
    cdef uint64_t _gen_clord_id(self):
        return 0

    cdef QCRecord * quote_get_subscribe(self, SmartOrderBase smart_order, char * v2_ticker, long ticker_id, int ticker_index)  except NULL:
        return NULL

    cdef int gate_send_order_new(self, SmartOrderBase smart_order, FIXNewOrderSingle order)  except -100:
        return -1
    cdef int gate_send_order_cancel(self, SmartOrderBase smart_order, FIXNewOrderSingle order)  except -100:
        return -1
    cdef int gate_send_order_replace(self, SmartOrderBase smart_order, FIXNewOrderSingle order, double price, double qty)  except -100:
        return -1
    cdef int gate_on_execution_report(self, FIXMsgStruct * exec_rep)  except -100:
        return -1