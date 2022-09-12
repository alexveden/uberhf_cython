from .fix_msg cimport FIXMsgStruct, FIXMsg
from uberhf.datafeed.quote_info import QuoteInfo
from libc.stdint cimport uint64_t
from uberhf.datafeed.quotes_cache cimport QCRecord


cdef class FIXNewOrderSingle:
    cdef FIXMsgStruct * msg
    cdef uint64_t clord_id
    cdef double price
    cdef int side
    cdef double qty
    cdef double cum_qty
    cdef double leaves_qty


    @staticmethod
    cdef FIXNewOrderSingle create(QCRecord * q,
                                  int account_id,
                                  double price,
                                  double qty,
                                  char order_type,
                                  char time_in_force,
                                  )

    cdef FIXMsgStruct * cancel_req(self)




