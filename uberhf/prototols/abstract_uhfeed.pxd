from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from libc.stdint cimport uint64_t
from uberhf.prototols.messages cimport ProtocolDSQuoteMessage, InstrumentInfo

cdef class UHFeedAbstract:
    cdef void register_datasource_protocol(self, object protocol)
    cdef void register_datafeed_protocol(self, object protocol)

    cdef void source_on_initialize(self, char * source_id, unsigned int source_life_id) nogil
    cdef void source_on_activate(self, char * source_id) nogil
    cdef void source_on_disconnect(self, char * source_id) nogil

    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfo * iinfo) nogil
    cdef void source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil

    cdef int feed_on_subscribe(self, char * v2_ticker, unsigned int client_life_id, bint is_subscribe) nogil

