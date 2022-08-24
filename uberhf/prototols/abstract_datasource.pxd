from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState

cdef class DatasourceAbstract:
    cdef source_send_initialize(self)
    cdef source_send_initialize_data(self)
    cdef source_send_activate(self)
    cdef source_send_disconnect(self)

    cdef void source_on_disconnect(self, ConnectionState * cstate) nogil

    cdef source_send_quote(self)
    cdef source_send_iinfo(self)
    cdef source_send_status(self)