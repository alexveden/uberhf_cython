from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from libc.stdint cimport uint64_t

cdef class DatasourceAbstract:
    cdef void register_datasource_protocol(self, object protocol)

    cdef void source_on_initialize(self) nogil
    cdef void source_on_disconnect(self) nogil
    cdef void source_on_activate(self) nogil

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil
