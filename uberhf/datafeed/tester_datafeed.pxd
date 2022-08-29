from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from libc.stdint cimport uint64_t
from .quotes_cache cimport SharedQuotesCache

cdef bint global_is_shutting_down = 0

cdef class DataFeedTester(FeedClientAbstract):
    cdef Transport transport_dealer
    cdef Transport transport_quote_sub
    cdef ProtocolDataFeed protocol
    cdef SharedQuotesCache qcache

    cdef bint is_shutting_down
    cdef int zmq_poll_timeout
    cdef zmq_pollitem_t zmq_poll_array[2]

    # Stats
    cdef int n_src_status
    cdef int n_quotes
    cdef int n_subscriptions_confirmations
    cdef int n_unsubscriptions_confirmations
    cdef int n_subscribe_sent
    cdef int n_subscribe_errors
    cdef int n_instrument_info


    cdef void register_datafeed_protocol(self, object protocol)

    # Server confirms subscription / unsubscription
    cdef void feed_on_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil

    # Server reports the datasource status has changed
    cdef void feed_on_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil

    # Subscribed updates
    cdef void feed_on_quote(self, int instrument_index) nogil

    cdef void feed_on_instrumentinfo(self, int instrument_index) nogil

    cdef void feed_on_initialize(self) nogil
    cdef void feed_on_activate(self) nogil
    cdef void feed_on_disconnect(self) nogil

    cdef int main(self) nogil

    cdef void close(self)