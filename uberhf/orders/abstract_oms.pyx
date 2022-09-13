from uberhf.datafeed.quotes_cache cimport QCRecord
from .fix_orders cimport FIXNewOrderSingle
from libc.stdint cimport uint64_t

cdef class OMSAbstract:
    cdef int order_register_single(self, FIXNewOrderSingle order):
        return -10000
    cdef uint64_t order_gen_clord_id(self):
        return 0