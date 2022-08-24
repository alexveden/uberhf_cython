from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState

cdef class UHFeedAbstract:
    cdef source_on_initialize(self)
    cdef source_on_initialize_data(self)
    cdef source_on_activate(self)
    cdef void source_on_disconnect(self, ConnectionState * cstate) nogil
    cdef source_on_quote(self)
    cdef source_on_iinfo(self)
    cdef source_on_status(self)

    cdef feed_initialize(self)
    cdef feed_subscribe(self)
    cdef feed_unsubscribe(self)
    cdef feed_disconnect(self)
