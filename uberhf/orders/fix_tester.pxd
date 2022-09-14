from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsg, FIXMsgStruct
from .fix_orders cimport FIXNewOrderSingle
from libc.stdint cimport uint64_t

cdef class FIXMsgC:
    cdef FIXMsgStruct * m

cdef class FIXTester(OMSAbstract):
    cdef int _clord_id_cnt

    cdef int order_register_single(self, FIXNewOrderSingle o)
    cdef int order_register_cxlrep(self, FIXNewOrderSingle order, FIXMsgStruct * m)

    cdef uint64_t order_gen_clord_id(self)

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