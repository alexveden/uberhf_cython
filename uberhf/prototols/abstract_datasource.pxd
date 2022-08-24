from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState

cdef class DatasourceAbstract:
    cdef int source_client_initialize(self) nogil
    cdef source_send_initialize_data(self)
    cdef source_send_activate(self)
    cdef source_send_disconnect(self)

    cdef void source_on_disconnect(self) nogil
    cdef void source_on_activate(self) nogil

    cdef source_send_quote(self)
    cdef source_send_iinfo(self)
    cdef source_send_status(self)