from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.includes.asserts cimport cyassert
from .protocol_datasource cimport HeartbeatConnectMessage, SourceStatus, SourceState
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE

DEF MSGT_HEARTBEAT = b'H'

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


cdef class ProtocolDatasourceClient:

    def __cinit__(self, Transport transport, object core):
        self.core = core
        self.transport = transport
        self.protocol_id = PROTOCOL_ID_DATASOURCE
        self.client_life_id = gen_lifetime_id(MODULE_ID_UHFEED)
        self.server_life_id = 0
        self.status = SourceStatus.inactive

    cdef int req_connect_heartbeat(self):
        cdef HeartbeatConnectMessage *msg = <HeartbeatConnectMessage*> malloc(sizeof(HeartbeatConnectMessage))
        msg.header.msg_type = MSGT_HEARTBEAT
        msg.header.sender_life_id = self.client_life_id
        msg.header.foreign_life_id = self.server_life_id
        msg.sender_status = self.status

        return self.transport.send(NULL, msg, sizeof(HeartbeatConnectMessage), no_copy=True)


    cdef int on_rep_connect_heartbeat(self, HeartbeatConnectMessage *msg):
        self.server_life_id = msg.header.foreign_life_id
        self.status = msg.sender_status

    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC:
        cdef int test = SourceStatus.connecting

        cdef TransportHeader * hdr = <TransportHeader *> msg

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return 0

        if hdr.msg_type == MSGT_HEARTBEAT:
            if msg_size != sizeof(HeartbeatConnectMessage):
                return PROTOCOL_ERR_SIZE

            return self.on_rep_connect_heartbeat(<HeartbeatConnectMessage*> msg)
        else:
            return PROTOCOL_ERR_WRONG_TYPE

cdef class ProtocolDatasourceServer:

    def __cinit__(self, Transport transport, object core):
        self.core = core
        self.transport = transport
        self.protocol_id = PROTOCOL_ID_DATASOURCE
        self.server_life_id = gen_lifetime_id(MODULE_ID_UHFEED)

    cdef int rep_connect_heartbeat(self, SourceState* state) except PROTOCOL_ERR_GENERIC:
        cdef HeartbeatConnectMessage *msg = <HeartbeatConnectMessage*> malloc(sizeof(HeartbeatConnectMessage))
        msg.header.msg_type = MSGT_HEARTBEAT
        msg.header.sender_life_id = self.server_life_id
        msg.header.foreign_life_id = state.foreign_life_id
        msg.sender_status = state.status

        return self.transport.send(state.sender_id, msg, sizeof(HeartbeatConnectMessage), no_copy=True)


    cdef int on_req_connect_heartbeat(self, HeartbeatConnectMessage *msg):
        cdef SourceState * state = <SourceState *>self.connected_clients.get(msg.header.sender_id)

        if state == NULL:
            # Not found
            state = <SourceState*>malloc(sizeof(SourceState))
            strlcpy(state.sender_id, msg.header.sender_id, TRANSPORT_SENDER_SIZE + 1)
            state.status = SourceStatus.connecting
            state.foreign_life_id = msg.header.sender_life_id
            # Insert via copy
            self.connected_clients.set(state)
            free(state)

            # Get source state pointer, so we can edit it
            state = <SourceState *>self.connected_clients.get(msg.header.sender_id)
            cyassert(state != NULL)
        else:
            if msg.header.sender_life_id != state.foreign_life_id:
                # Possible foreign restart, lets make it re-initialize
                state.status = SourceStatus.inactive
                # TODO: core.reset_datasource()

        state.last_heartbeat_time_ns = datetime_nsnow()

        # Reply on heartbeat
        return self.rep_connect_heartbeat(state)


    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC:
        # TODO: do it once in the poller loop!
        #if msg_size < sizeof(TransportHeader):
        #    return PROTOCOL_ERR_SIZE
        cdef int test = SourceStatus.connecting

        cdef TransportHeader * hdr = <TransportHeader *> msg

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return 0

        if hdr.msg_type == MSGT_HEARTBEAT:
            if msg_size != sizeof(HeartbeatConnectMessage):
                return PROTOCOL_ERR_SIZE

            return self.on_req_connect_heartbeat(<HeartbeatConnectMessage*> msg)
        else:
            return PROTOCOL_ERR_WRONG_TYPE






