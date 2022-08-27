from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.uhfprotocols cimport *

cdef class FeedClientAbstract:
    cdef void register_datafeed_protocol(self, object protocol):
        raise NotImplementedError(f'You must implement this method in child class')

    # Server confirms subscription / unsubscription
    cdef void feed_on_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil:
        return

    # Server reports the datasource status has changed
    cdef void feed_on_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil:
        return

    # Subscribed updates
    cdef void feed_on_quote(self, int instrument_index, uint64_t instrument_id) nogil:
        return

    cdef void feed_on_instrumentinfo(self, int instrument_index, uint64_t instrument_id) nogil:
        return