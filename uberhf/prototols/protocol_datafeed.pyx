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
from uberhf.prototols.libzmq cimport *

DEF MSGT_SUBS = b's'
DEF MSGT_UPDATE = b'u'
DEF MSGT_SRCSTATUS = b'c'

cdef class ProtocolDataFeed(ProtocolBase):

    def __cinit__(self, module_id, transport, transport_pubsub, feed_client = None, feed_server = None, heartbeat_interval_sec=5):
        if feed_client is None and feed_server is None:
            raise ValueError(f'You must set one of feed_client or feed_server')
        elif feed_client is not None and feed_server is not None:
            raise ValueError(f'Arguments are mutually exclusive: feed_client, feed_server')

        if feed_client is not None:
            is_server = 0
            assert transport_pubsub.socket_type == ZMQ_SUB, f'Client transport must be a ZMQ_SUB'
            self.feed_client = feed_client
            self.feed_client.register_datafeed_protocol(self)

        elif feed_server is not None:
            is_server = 1
            assert transport_pubsub.socket_type == ZMQ_PUB, f'Server transport must be a ZMQ_PUB'
            self.feed_server = feed_server
            self.feed_server.register_datafeed_protocol(self)
        else:
            raise NotImplementedError()

        # Calling super() class in Cython must be by static
        ProtocolBase.protocol_initialize(self, PROTOCOL_ID_DATAFEED, is_server, module_id, transport, heartbeat_interval_sec)
        self.pubsub_transport = transport

    cdef int send_subscribe(self, char * v2_ticker) nogil:
        return -1
    cdef int send_unsubscribe(self, char * v2_ticker) nogil:
        return -1
    cdef int send_subscribe_confirm(self, char * v2_ticker, uint64_t instrument_id, int retcode) nogil:
        return -1

    # From server to client via pub-sub
    cdef int send_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil:
        return -1

    cdef int send_feed_update(self, int instrument_index, uint64_t instrument_id, int update_type) nogil:
        return -1

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
