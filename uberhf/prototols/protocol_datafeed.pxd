from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.datafeed.uhffeed cimport Quote
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, TransportHeader, ProtocolDFSubscribeMessage


cdef class ProtocolDataFeed(ProtocolBase):
    cdef FeedClientAbstract feed_client
    cdef UHFeedAbstract feed_server
    cdef Transport pubsub_transport

    # From client to server via dealer - router
    cdef int send_subscribe(self, char * v2_ticker) nogil
    cdef int send_unsubscribe(self, char * v2_ticker) nogil
    cdef int send_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil

    cdef int _send_subscribe(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil

    # From server to client via pub-sub
    cdef int send_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil
    cdef int send_feed_update(self, int instrument_index, uint64_t instrument_id, int update_type) nogil

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
