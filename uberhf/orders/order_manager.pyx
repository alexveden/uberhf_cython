from .orders cimport OrderTimeInForce


cpdef main():
    cdef char ord_tyme = OrderTimeInForce.DAY