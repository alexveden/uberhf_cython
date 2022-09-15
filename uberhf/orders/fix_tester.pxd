from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsg, FIXMsgStruct
from .fix_orders cimport FIXNewOrderSingle
from libc.stdint cimport uint64_t
from uberhf.datafeed.quotes_cache cimport QCRecord
from .smart_order_base cimport SmartOrderBase

cdef class FIXMsgC:
    cdef FIXMsgStruct * m

cdef class FIXTester(OMSAbstract):
    cdef int _clord_id_cnt
    cdef readonly dict data2smart
    cdef readonly dict smart2orders
    cdef readonly dict orders2smart
    cdef readonly dict data_cache
    cdef readonly dict actions
    cdef SmartOrderBase smart_order


    cdef QCRecord * quote_get_subscribe(self, SmartOrderBase smart_order, char * v2_ticker, long ticker_id, int ticker_index)  except NULL
    cdef int gate_send_order_new(self, SmartOrderBase smart_order, FIXNewOrderSingle order) except -100
    cdef int gate_send_order_cancel(self, SmartOrderBase smart_order, FIXNewOrderSingle order)  except -100
    cdef int gate_send_order_replace(self, SmartOrderBase smart_order, FIXNewOrderSingle order, double price, double qty)  except -100
    cdef int gate_on_execution_report(self, FIXMsgStruct * exec_rep)  except -100


    cdef int order_register_single(self, FIXNewOrderSingle o)
    cdef int order_register_cxlrep(self, FIXNewOrderSingle order, FIXMsgStruct * m)

    cdef uint64_t _gen_clord_id(self)

    cdef FIXMsgC fix_cxl_request(self, FIXNewOrderSingle order)
    cdef FIXMsgC fix_rep_request(self, FIXNewOrderSingle order, double price = *, double qty = *)

    cdef FIXMsgC fix_cxlrep_reject_msg(self,
                                       FIXMsgC cancel_msg,
                                       char ord_status,
                                       )

    cdef FIXMsgC fix_exec_report_msg(self,
                             FIXNewOrderSingle order,
                             uint64_t clord_id,
                             char exec_type,
                             char ord_status,
                             double cum_qty = *,
                             double leaves_qty = *,
                             double last_qty = *,
                             double price = *,
                             double order_qty = *,
                             uint64_t orig_clord_id = *,
                            )