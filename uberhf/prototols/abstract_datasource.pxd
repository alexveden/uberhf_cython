from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from libc.stdint cimport uint64_t

cdef class DatasourceAbstract:
    cdef int source_on_initialize(self) nogil
    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil
    cdef source_send_activate(self)
    cdef source_send_disconnect(self)

    cdef void source_on_disconnect(self) nogil
    cdef void source_on_activate(self) nogil

    cdef source_send_quote(self)
    cdef source_send_iinfo(self)
    cdef source_send_status(self)