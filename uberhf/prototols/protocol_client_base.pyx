from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.includes.uhfprotocols cimport *


ctypedef struct HeartbeatConnectMessage:
    TransportHeader header
    int client_id
    int server_id


cdef class EventHandlerBase:
    cdef int on_req_connect_heartbeat(self, HeartbeatConnectMessage * msg) except PROTOCOL_ERR_GENERIC:
        return 1

cdef class ProtocolClientBase:
    cdef char protocol_id
    cdef Transport transport
    cdef readonly EventHandlerBase evnt

    def __cinit__(self, Transport transport, EventHandlerBase event_handler):
        self.evnt = event_handler
        self.transport = transport
        self.protocol_id = PROTOCOL_ID_NONE

    cdef int req_connect_heartbeat(self, int client_id, int server_id) nogil except PROTOCOL_ERR_GENERIC:
        cdef HeartbeatConnectMessage *msg = <HeartbeatConnectMessage*> malloc(sizeof(HeartbeatConnectMessage))
        msg.header.msg_type = PROTOCOL_MSGT_HEARTBEAT
        msg.header.my_id = client_id
        msg.header.foreign_id = server_id

        msg.client_id = client_id
        msg.server_id = server_id

        return self._send_no_copy(msg, sizeof(HeartbeatConnectMessage))

    cdef int _send_no_copy(self, void *msg, size_t msg_size) nogil except PROTOCOL_ERR_GENERIC:
        cyassert(msg_size > sizeof(TransportHeader))   # Message is too small!

        cdef TransportHeader * hdr = <TransportHeader*> msg

        cyassert(self.protocol_id != PROTOCOL_ID_NONE)  # You must set a protocol in child class!
        hdr.protocol_id = self.protocol_id

        return self.transport.send(NULL, msg, msg_size, no_copy=True)

    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC:
        # TODO: do it once in the poller loop!
        #if msg_size < sizeof(TransportHeader):
        #    return PROTOCOL_ERR_SIZE

        cdef TransportHeader * hdr = <TransportHeader *> msg

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return 0

        if hdr.msg_type == PROTOCOL_MSGT_HEARTBEAT:
            if msg_size != sizeof(HeartbeatConnectMessage):
                return PROTOCOL_ERR_SIZE

            return self.evnt.on_req_connect_heartbeat(<HeartbeatConnectMessage*> msg)
        else:
            return PROTOCOL_ERR_WRONG_TYPE






