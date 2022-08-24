from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from libc.stdint cimport uint64_t

cdef class UHFeedAbstract:
    cdef int source_on_initialize(self, ConnectionState * cstate) nogil
    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id) nogil
    cdef source_on_activate(self)
    cdef void source_on_disconnect(self, ConnectionState * cstate) nogil
    cdef source_on_quote(self)
    cdef source_on_iinfo(self)
    cdef source_on_status(self)

    cdef feed_initialize(self)
    cdef feed_subscribe(self)
    cdef feed_unsubscribe(self)
    cdef feed_disconnect(self)
