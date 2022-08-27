from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.uhfprotocols cimport *

cdef class FeedClientAbstract:
    cdef void register_datafeed_protocol(self, object protocol)

    # Server confirms subscription / unsubscription
    cdef void feed_on_subscribe_confirm(self, char * v2_ticker, uint64_t instrument_id, int retcode) nogil

    # Server reports the datasource status has changed
    cdef void feed_on_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil

    # Subscribed updates
    cdef void feed_on_quote(self, int instrument_index, uint64_t instrument_id) nogil

    cdef void feed_on_instrumentinfo(self, int instrument_index, uint64_t instrument_id) nogil