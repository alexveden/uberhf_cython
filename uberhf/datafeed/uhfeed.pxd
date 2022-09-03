from libc.stdint cimport uint64_t
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN, TRANSPORT_SENDER_SIZE, ProtocolStatus
from uberhf.prototols.messages cimport Quote, InstrumentInfoStruct, ProtocolDSQuoteMessage
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSource
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from uberhf.prototols.transport cimport Transport
from .quotes_cache cimport SharedQuotesCache
from uberhf.prototols.libzmq cimport *

cdef bint global_is_shutting_down = 0

cdef class UHFeed(UHFeedAbstract):
    cdef unsigned int quotes_received
    cdef unsigned int quotes_emitted
    cdef unsigned int quotes_errors
    cdef unsigned int source_errors
    cdef unsigned int feed_errors
    cdef unsigned int uhfeed_life_id
    cdef bint is_shutting_down
    cdef SharedQuotesCache quote_cache
    cdef ProtocolDataSource protocol_source
    cdef ProtocolDataFeed protocol_feed
    cdef Transport transport_router
    cdef Transport transport_pub

    cdef int zmq_poll_timeout
    cdef zmq_pollitem_t zmq_poll_array[1]

    cdef void register_datasource_protocol(self, object protocol)
    cdef void register_datafeed_protocol(self, object protocol)

    cdef void source_on_initialize(self, char * source_id, unsigned int source_life_id) nogil
    cdef void source_on_activate(self, char * source_id) nogil
    cdef void source_on_disconnect(self, char * source_id) nogil

    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfoStruct * iinfo) nogil
    cdef void source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil

    cdef void feed_on_initialize(self, char * feed_id) nogil
    cdef void feed_on_activate(self, char * feed_id) nogil
    cdef void feed_on_disconnect(self, char * feed_id) nogil
    cdef int feed_on_subscribe(self, char * v2_ticker, unsigned int client_life_id, bint is_subscribe) nogil

    cdef int main(self) nogil

    cdef void close(self)