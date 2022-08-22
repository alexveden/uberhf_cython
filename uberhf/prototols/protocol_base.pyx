from uberhf.includes.uhfprotocols cimport ProtocolStatus, TRANSPORT_SENDER_SIZE, PROTOCOL_ID_BASE, PROTOCOL_ERR_WRONG_ORDER
from .transport cimport Transport, TransportHeader
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from libc.stdlib cimport malloc, free
from uberhf.includes.hashmap cimport HashMap
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.prototols.libzmq cimport *

DEF MSGT_CONNECT = b'C'
DEF MSGT_HEARTBEAT = b'H'

cdef class ProtocolBase:
    def __cinit__(self, is_server, module_id, transport):
        self.initialize(is_server, module_id, transport)

    cdef void initialize(self, bint is_server, int module_id, Transport transport):
        assert module_id >0 and module_id < 40, 'Module ID must be >0 and < 40'

        self.is_server = is_server
        if is_server:
            assert (transport.socket_type == ZMQ_ROUTER), f'Server transport must be ZMQ_ROUTER'
            self.server_life_id = gen_lifetime_id(module_id)
            self.client_life_id = 0
        else:
            assert (transport.socket_type == ZMQ_DEALER), f'Server transport must be ZMQ_DEALER'
            self.client_life_id = gen_lifetime_id(module_id)
            self.server_life_id = 0

        self.transport = transport
        self.connections = HashMap(sizeof(ConnectionState))

    cdef ConnectionState * get_state(self, char * sender_id):
        cyassert (sender_id != NULL)
        if self.is_server:
            cyassert(len(sender_id) > 0)
        else:
            cyassert(len(sender_id) == 0)


        cdef ConnectionState* cstate = <ConnectionState*> self.connections.get(sender_id)
        if cstate == NULL:
            # Now initialized state!
            state = <ConnectionState *> malloc(sizeof(ConnectionState))
            strlcpy(state.sender_id, sender_id, TRANSPORT_SENDER_SIZE + 1)
            state.status = ProtocolStatus.UHF_INACTIVE
            state.server_life_id = self.server_life_id
            state.client_life_id = self.client_life_id
            state.last_heartbeat_time_ns = 0
            state.msg_sent = 0
            state.msg_recvd = 0
            state.msg_errs = 0

            # Insert via copy
            self.connections.set(state)
            free(state)

            cstate = <ConnectionState*> self.connections.get(sender_id)
            cyassert(state != NULL)

        return cstate

    cdef int connect(self):
        cyassert(self.is_server == 0) # Only client can connect to the server!

        cdef ConnectionState * cstate = self.get_state(b'')
        cyassert(cstate.status == ProtocolStatus.UHF_INACTIVE)

        cstate.msg_sent += 1
        cstate.status = ProtocolStatus.UHF_CONNECTING
        cstate.last_heartbeat_time_ns = datetime_nsnow()


        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        msg.header.protocol_id = PROTOCOL_ID_BASE
        msg.header.msg_type = MSGT_CONNECT
        msg.header.server_life_id = cstate.server_life_id
        msg.header.client_life_id = self.client_life_id
        msg.status = cstate.status

        return self.transport.send(NULL, msg, sizeof(ProtocolBaseMessage), no_copy=True)

    cdef int on_connect(self, ProtocolBaseMessage * msg):
        """
        Client / server `connect` event handler
        :param msg: 
        :return: 
        """
        #cybreakpoint(1)
        cdef ConnectionState * cstate
        cdef ProtocolBaseMessage *msg_out

        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        if cstate.status != ProtocolStatus.UHF_ACTIVE:
            cstate.status = ProtocolStatus.UHF_CONNECTING
            cstate.last_heartbeat_time_ns = datetime_nsnow()

            if self.is_server:
                #
                # Server must send a reply with its life id
                #
                cstate.client_life_id = msg.header.client_life_id
                cstate.msg_recvd += 1
                cstate.msg_sent += 1

                msg_out = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
                msg_out.header.protocol_id = PROTOCOL_ID_BASE
                msg_out.header.msg_type = MSGT_CONNECT
                msg_out.header.server_life_id = self.server_life_id
                msg_out.header.client_life_id = cstate.client_life_id
                msg_out.status = cstate.status

                return self.transport.send(cstate.sender_id, msg_out, sizeof(ProtocolBaseMessage), no_copy=True)
            else:
                #
                # Client doesn't reply but can continue initialization after that
                #
                cyassert(cstate.status == ProtocolStatus.UHF_CONNECTING) # Expected get ProtocolStatus.UHF_CONNECTING from server
                cstate.msg_recvd += 1
                cstate.server_life_id = msg.header.server_life_id
                return 1
        else:
            #
            cstate.msg_errs += 1
            return PROTOCOL_ERR_WRONG_ORDER
