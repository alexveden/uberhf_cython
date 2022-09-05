cdef class _OrderTimeInForce:
    cdef readonly char DAY
    cdef readonly char IOC
    cdef readonly char FOK


cdef _OrderTimeInForce OrderTimeInForce