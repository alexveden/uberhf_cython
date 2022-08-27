from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.stdlib cimport malloc, free
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.prototols.messages cimport ProtocolDFSubscribeMessage
from uberhf.prototols.libzmq cimport *
from uberhf.includes.utils cimport strlcpy

DEF MSGT_SUBS = b's'
DEF MSGT_UPDATE = b'u'
DEF MSGT_SRCSTATUS = b'c'

cdef class ProtocolDataFeed(ProtocolBase):

    def __cinit__(self, module_id, transport, transport_pubsub, feed_client = None, feed_server = None, heartbeat_interval_sec=5):
        if feed_client is None and feed_server is None:
            raise ValueError(f'You must set one of feed_client or feed_server')
        elif feed_client is not None and feed_server is not None:
            raise ValueError(f'Arguments are mutually exclusive: feed_client, feed_server')

        if strcmp(transport_pubsub.transport_id, transport.transport_id) != 0:
            raise ValueError(f'Both transports must have identical `transport_id`')

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
        cyassert(self.is_server == 0)  # Only clients allowed
        return self._send_subscribe(v2_ticker, -1, 1)

    cdef int send_unsubscribe(self, char * v2_ticker) nogil:
        cyassert(self.is_server == 0)  # Only clients allowed
        return self._send_subscribe(v2_ticker, -1, 0)

    cdef int send_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil:
        cyassert(self.is_server == 1)  # Only servers allowed
        return self._send_subscribe(v2_ticker, instrument_index, is_subscribe)

    cdef int _send_subscribe(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil:
        cdef ConnectionState * cstate = self.get_state(b'')

        if cstate.status != ProtocolStatus.UHF_ACTIVE:
            return PROTOCOL_ERR_WRONG_ORDER

        if v2_ticker == NULL or strlen(v2_ticker) > V2_TICKER_MAX_LEN - 1:
            return PROTOCOL_ERR_ARG_ERR

        cdef ProtocolDFSubscribeMessage *msg_out = <ProtocolDFSubscribeMessage *> malloc(sizeof(ProtocolDFSubscribeMessage))
        msg_out.header.protocol_id = self.protocol_id
        msg_out.header.msg_type = MSGT_SUBS
        msg_out.header.server_life_id = cstate.server_life_id
        msg_out.header.client_life_id = cstate.client_life_id

        strlcpy(msg_out.v2_ticker, v2_ticker, V2_TICKER_MAX_LEN)
        msg_out.is_subscribe = is_subscribe
        msg_out.instrument_index = instrument_index

        return self.transport.send(NULL, msg_out, sizeof(ProtocolDFSubscribeMessage), no_copy=1)

    # From server to client via pub-sub
    cdef int send_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil:
        return -1

    cdef int send_feed_update(self, int instrument_index, uint64_t instrument_id, int update_type) nogil:
        return -1

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil:
        cdef TransportHeader * hdr = <TransportHeader *> msg
        cdef ProtocolDFSubscribeMessage * msg_sub
        cdef int rc = 0

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return rc

        # In order from the most frequent to less frequent
        if hdr.msg_type == MSGT_SUBS:
            if msg_size != sizeof(ProtocolDFSubscribeMessage):
                return PROTOCOL_ERR_SIZE

            msg_sub = <ProtocolDFSubscribeMessage *> msg
            if self.is_server:
                rc = self.feed_server.feed_on_subscribe(msg_sub.v2_ticker, msg_sub.header.client_life_id, msg_sub.is_subscribe)

                return self.send_subscribe_confirm(msg_sub.v2_ticker,
                                                   rc,
                                                   msg_sub.is_subscribe,
                                                   )
            else:
                self.feed_client.feed_on_subscribe_confirm(msg_sub.v2_ticker,
                                                           msg_sub.instrument_index,
                                                           msg_sub.is_subscribe,
                                                           )
                return 1

        # elif hdr.msg_type == MSGT_IINFO:
        #     cyassert(0)

        else:
            rc = ProtocolBase.on_process_new_message(self, msg, msg_size)
            cyassert(rc != 0)

        return rc
