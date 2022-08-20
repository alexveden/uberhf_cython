from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy, datetime_nsnow
from uberhf.includes.asserts cimport cyassert
from .protocol_datasource cimport HeartbeatConnectMessage, SourceStatus, SourceState
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE




cdef class HashMapDataSources(HashMapBase):
    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil:
        cdef SourceState *ta = <SourceState*>a
        cdef SourceState *tb = <SourceState*>b
        return strcmp(ta[0].sender_id, tb[0].sender_id)

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil:
        cdef SourceState *t = <SourceState*>item
        return HashMapBase.hash_func(t[0].sender_id, strlen(t[0].sender_id), seed0, seed1)

    def __cinit__(self):
        self._new(sizeof(SourceState), self.item_hash, self.item_compare, 16)


cdef class ProtocolDatasourceServer:
    cdef char protocol_id
    cdef Transport transport
    cdef readonly object core
    cdef int server_life_id
    cdef HashMapDataSources connected_clients

    def __cinit__(self, Transport transport, object core):
        self.core = core
        self.transport = transport
        self.protocol_id = PROTOCOL_ID_NONE

    cdef int rep_connect_heartbeat(self, char *sender_id, int foreign_life_id) nogil except PROTOCOL_ERR_GENERIC:
        cdef HeartbeatConnectMessage *msg = <HeartbeatConnectMessage*> malloc(sizeof(HeartbeatConnectMessage))
        msg.header.msg_type = PROTOCOL_MSGT_HEARTBEAT
        msg.header.sender_life_id = self.server_life_id
        msg.header.foreign_life_id = foreign_life_id

        return self.transport.send(sender_id, msg, sizeof(HeartbeatConnectMessage), no_copy=True)


    cdef int on_req_connect_heartbeat(self, HeartbeatConnectMessage *msg):
        cdef SourceState * src = <SourceState *>self.connected_clients.get(msg.header.sender_id)
        if src == NULL:
            # Not found
            src = <SourceState*>malloc(sizeof(SourceState))
            strlcpy(src.sender_id, msg.header.sender_id, TRANSPORT_SENDER_SIZE + 1)
            src.status = SourceStatus.inactive
            src.foreign_life_id = msg.header.sender_life_id
            # Insert via copy
            self.connected_clients.set(src)
            free(src)

            # Get source state pointer, so we can edit it
            src = <SourceState *>self.connected_clients.get(msg.header.sender_id)
            cyassert(src != NULL)
        else:
            if msg.header.sender_life_id != src.foreign_life_id:
                # Possible foreign restart, lets make it re-initialize
                src.status = SourceStatus.inactive
                # TODO: core.reset_datasource()

        src.last_heartbeat_time_ns = datetime_nsnow()
        return self.rep_connect_heartbeat(msg.header.sender_id, src.foreign_life_id)


    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC:
        # TODO: do it once in the poller loop!
        #if msg_size < sizeof(TransportHeader):
        #    return PROTOCOL_ERR_SIZE
        cdef int test = SourceStatus.connecting

        cdef TransportHeader * hdr = <TransportHeader *> msg

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return 0

        if hdr.msg_type == PROTOCOL_MSGT_HEARTBEAT:
            if msg_size != sizeof(HeartbeatConnectMessage):
                return PROTOCOL_ERR_SIZE

            return self.on_req_connect_heartbeat(<HeartbeatConnectMessage*> msg)
        else:
            return PROTOCOL_ERR_WRONG_TYPE






