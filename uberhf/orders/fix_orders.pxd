from .fix_binary_msg cimport FIXBinaryMsg
from uberhf.datafeed.quote_info import QuoteInfo

cdef class FIXNewOrderSingle:
    cdef FIXBinaryMsg msg
    cdef object q
    cdef object _clord_cached
    cdef object _price_cached
    cdef object _qty_cached
    cdef object _side_cached

    cpdef FIXBinaryMsg cancel_req(self, bytes req_clord_id)

cdef class FIXNewOrderSinglePy:
    cdef readonly double qty
    cdef readonly double px
    cdef readonly bytes clord_id
    cdef bytes account
    cdef object q