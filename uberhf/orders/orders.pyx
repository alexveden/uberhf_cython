cdef class _OrderTimeInForce:
    def __cinit__(self):
        self.DAY = b'0'
        self.IOC = b'3'
        self.FOK = b'4'


cdef _OrderTimeInForce OrderTimeInForce = _OrderTimeInForce()