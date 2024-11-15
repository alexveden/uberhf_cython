from uberhf.datafeed.quotes_cache cimport QCRecord
from .fix_orders cimport FIXNewOrderSingle
from .fix_msg cimport FIXMsgStruct
from libc.stdint cimport uint64_t
from .smart_order_base cimport SmartOrderBase

cdef class OMSAbstract:
    cdef uint64_t _gen_clord_id(self)

    cdef QCRecord * quote_get_subscribe(self, SmartOrderBase smart_order, char * v2_ticker, long ticker_id, int ticker_index)  except NULL
    cdef int gate_send_order_new(self, SmartOrderBase smart_order, FIXNewOrderSingle order)  except -100
    cdef int gate_send_order_cancel(self, SmartOrderBase smart_order, FIXNewOrderSingle order)   except -100
    cdef int gate_send_order_replace(self, SmartOrderBase smart_order, FIXNewOrderSingle order, double price, double qty)   except -100
    cdef int gate_on_execution_report(self, FIXMsgStruct * exec_rep)   except -100

    #
    # cdef int strategy_on_new(self)
    # cdef int strategy_send_execution_report(self)
    # cdef int strategy_send_cxlrej_report(self)
    # cdef int strategy_on_cancel_request(self)
    # cdef int strategy_on_replace_request(self)
    #
    #
    # cdef void feed_on_quote(self, int instrument_index) nogil
    # cdef void feed_on_instrumentinfo(self, int instrument_index) nogil
    #

