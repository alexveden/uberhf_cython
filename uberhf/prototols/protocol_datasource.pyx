from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *


ctypedef struct HeartbeatConnectMessage:
    TransportHeader header
    int client_id
    int server_id

cdef class ProtocolDatasourceServer:
    cdef char protocol_id
    cdef Transport transport
    cdef readonly object core

    def __cinit__(self, Transport transport, object core):
        self.core = core
        self.transport = transport
        self.protocol_id = PROTOCOL_ID_NONE

    #
    # cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC:
    #     # TODO: do it once in the poller loop!
    #     #if msg_size < sizeof(TransportHeader):
    #     #    return PROTOCOL_ERR_SIZE
    #
    #     cdef TransportHeader * hdr = <TransportHeader *> msg
    #
    #     if hdr.protocol_id != self.protocol_id:
    #         # Protocol doesn't match
    #         return 0
    #
    #     if hdr.msg_type == PROTOCOL_MSGT_HEARTBEAT:
    #         if msg_size != sizeof(HeartbeatConnectMessage):
    #             return PROTOCOL_ERR_SIZE
    #
    #         return self.evnt.on_req_connect_heartbeat(<HeartbeatConnectMessage*> msg)
    #     else:
    #         return PROTOCOL_ERR_WRONG_TYPE






