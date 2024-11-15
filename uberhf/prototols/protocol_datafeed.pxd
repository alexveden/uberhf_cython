from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, TransportHeader, ProtocolDFSubscribeMessage


cdef class ProtocolDataFeed(ProtocolBase):
    cdef int module_subs_id
    cdef FeedClientAbstract feed_client
    cdef UHFeedAbstract feed_server
    cdef Transport pubsub_transport

    cdef void initialize_client(self, ConnectionState * cstate) nogil
    cdef void activate_client(self, ConnectionState * cstate) nogil
    cdef void disconnect_client(self, ConnectionState * cstate) nogil

    # From client to server via dealer - router
    cdef int send_subscribe(self, char * v2_ticker) nogil
    cdef int send_unsubscribe(self, char * v2_ticker) nogil

    cdef int _send_subscribe(self, char * sender_id, char * v2_ticker, int instrument_index, bint is_subscribe) nogil

    # From server to client via pub-sub
    cdef int send_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil
    cdef int send_feed_update(self, int instrument_index, int update_type, uint64_t subscriptions_bits) nogil

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
