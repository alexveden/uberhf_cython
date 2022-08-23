from uberhf.includes.uhfprotocols cimport ProtocolStatus, TRANSPORT_SENDER_SIZE, PROTOCOL_ID_BASE, PROTOCOL_ERR_WRONG_ORDER, PROTOCOL_ERR_LIFE_ID
from .transport cimport Transport, TransportHeader
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from libc.stdlib cimport malloc, free
from libc.string cimport strlen
from uberhf.includes.hashmap cimport HashMap
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.prototols.libzmq cimport *

DEF MSGT_CONNECT = b'C'
DEF MSGT_ACTIVATE = b'A'
DEF MSGT_DISCONNECT = b'D'
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

    cdef ConnectionState * get_state(self, char * sender_id) nogil:
        cyassert (sender_id != NULL)
        if self.is_server:
            cyassert(strlen(sender_id) > 0)
        else:
            cyassert(strlen(sender_id) == 0)


        cdef ConnectionState* cstate = <ConnectionState*> self.connections.get(sender_id)
        if cstate == NULL:
            # Now initialized state!
            state = <ConnectionState *> malloc(sizeof(ConnectionState))
            strlcpy(state.sender_id, sender_id, TRANSPORT_SENDER_SIZE + 1)
            state.status = ProtocolStatus.UHF_INACTIVE
            state.server_life_id = self.server_life_id
            state.client_life_id = self.client_life_id
            state.last_msg_time_ns = 0
            state.msg_sent = 0
            state.msg_recvd = 0
            state.msg_errs = 0
            state.n_heartbeats = 0

            # Insert via copy
            self.connections.set(state)
            free(state)

            cstate = <ConnectionState*> self.connections.get(sender_id)
            cyassert(state != NULL)

        return cstate

    #
    #  Base protocol commandd
    #
    cdef int send_connect(self) nogil:
        """
        Client connection to the server
        
        :return: 
        """
        cyassert(self.is_server == 0) # Only client can connect to the server!

        cdef ConnectionState * cstate = self.get_state(b'')
        cyassert(cstate.status == ProtocolStatus.UHF_INACTIVE)
        return self._send_command(cstate, ProtocolStatus.UHF_CONNECTING, MSGT_CONNECT)

    cdef int send_activate(self) nogil:
        """
        Client connection activation
        
        :return: 
        """
        cyassert(self.is_server == 0)  # Only client can connect to the server!
        cdef ConnectionState * cstate = self.get_state(b'')
        return self._send_command(cstate, ProtocolStatus.UHF_ACTIVE, MSGT_ACTIVATE)

    cdef int send_disconnect(self) nogil:
        """
        Client disconnection
        
        :return: 
        """
        cyassert(self.is_server == 0)  # Only client can connect to the server!
        cdef ConnectionState * cstate = self.get_state(b'')
        cstate.server_life_id = 0
        cstate.status = ProtocolStatus.UHF_INACTIVE
        return self._send_command(cstate, ProtocolStatus.UHF_INACTIVE, MSGT_DISCONNECT)

    cdef int send_heartbeat(self) nogil:
        """
        Client heartbeat
        
        :return: 
        """
        cyassert(self.is_server == 0)  # Only client can connect to the server!
        cdef ConnectionState * cstate = self.get_state(b'')
        return self._send_command(cstate, ProtocolStatus.UHF_ACTIVE, MSGT_HEARTBEAT)

    #
    #
    # EVENT HANDLERS
    #
    #
    cdef int on_connect(self, ProtocolBaseMessage * msg) nogil:
        """
        Client / server `connect` event handler
        
        :param msg: 
        :return: 
        """
        #cybreakpoint(1)
        cdef ConnectionState * cstate

        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        return self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_CONNECTING, 0, MSGT_CONNECT)


    cdef int on_activate(self, ProtocolBaseMessage * msg) nogil:
        """
        Client / server `activate` event handler
        
        :param msg: 
        :return: 
        """
        #cybreakpoint(1)
        cdef ConnectionState * cstate

        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        return self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_ACTIVE, 1, MSGT_ACTIVATE)

    cdef int on_disconnect(self, ProtocolBaseMessage * msg) nogil:
        """
        Client / server `disconnect` event handler
        
        :param msg: 
        :return: 
        """
        cyassert(self.is_server == 1) # Only servers must receive this command!

        cdef ConnectionState * cstate

        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        return self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_INACTIVE, 0, 0)


    cdef int on_heartbeat(self, ProtocolBaseMessage * msg) nogil:
        """
        A heartbeat message for maintaining connection granularity
        
        :param msg: 
        :return: 
        """
        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        cdef int rc = self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_ACTIVE, 1, MSGT_HEARTBEAT)
        if rc > 0:
            cstate.n_heartbeats += 1
        return rc

    #
    # Private methods
    #
    cdef int _send_command(self, ConnectionState * cstate, ProtocolStatus new_status, char msg_type) nogil:
        """
        Send generic client command
        
        :param cstate: current client connection state
        :param new_status: expected next status transition
        :param msg_type: message type to send to the server
        :return: >0 on success, <= 0 on error
        """
        cyassert(self.is_server == 0)  # Only client can connect to the server!

        cdef ProtocolStatus next_state = self._state_transition(cstate.status, new_status)
        cyassert(next_state == new_status)
        if next_state != new_status:
            # Just is case if assert compiled out
            return PROTOCOL_ERR_WRONG_ORDER

        cdef ProtocolBaseMessage *msg = self._make_msg(cstate, msg_type, next_state)
        cyassert(msg != NULL)
        cstate.msg_sent += 1
        return self.transport.send(NULL, msg, sizeof(ProtocolBaseMessage), no_copy=True)

    cdef int _on_msg_reply(self,
                           ConnectionState * cstate,
                           ProtocolBaseMessage *msg,
                           ProtocolStatus expected_status,
                           bint check_life_id,
                           char server_reply_msg_type,
                           ) nogil:
        """
        Generic reply handler for base protocol
        
        :param cstate: current state 
        :param msg: incoming message
        :param expected_status: expected status of `next_state` transition
        :param check_life_id:  check if client_life_id / server_life_id match
        :param server_reply_msg_type: if not 0, server must reply with a message type `server_reply_msg_type`
        :return: > 0 if success, <= 0 on error
        """

        cdef ProtocolStatus next_state = self._state_transition(cstate.status, msg.status)
        cdef bint is_valid_life_id = 1
        if check_life_id:
            is_valid_life_id = self._check_life_id(cstate, msg)

        if next_state == expected_status and is_valid_life_id:
            cstate.last_msg_time_ns = datetime_nsnow()
            cstate.status = next_state
            cstate.msg_recvd += 1

            if self.is_server:
                #
                # Server must send a reply with its life id
                #
                if next_state == ProtocolStatus.UHF_INACTIVE:
                    cstate.client_life_id = 0
                else:
                    cstate.client_life_id = msg.header.client_life_id

                if server_reply_msg_type != 0:
                    cstate.msg_sent += 1
                    return self.transport.send(cstate.sender_id,
                                               self._make_msg(cstate, server_reply_msg_type, next_state),
                                               sizeof(ProtocolBaseMessage),
                                               no_copy=True)
                else:
                    return 1
            else:
                #
                # Client doesn't reply but can continue initialization after that
                #
                if next_state == ProtocolStatus.UHF_INACTIVE:
                    cstate.server_life_id = 0
                else:
                    cstate.server_life_id = msg.header.server_life_id
                return 1
        else:
            # Error in state transition
            cyassert(next_state == ProtocolStatus.UHF_INACTIVE)
            cstate.status = next_state
            cstate.msg_errs += 1
            if self.is_server:
                cstate.client_life_id = 0
            else:
                cstate.server_life_id = msg.header.server_life_id

            if is_valid_life_id:
                return PROTOCOL_ERR_WRONG_ORDER
            else:
                return PROTOCOL_ERR_LIFE_ID


    cdef bint _check_life_id(self, ConnectionState *cstate, ProtocolBaseMessage *msg) nogil:
        """
        Make sure if server and client life id match
        
        :param cstate: current state
        :param msg: incoming message
        :return: 
        """
        if self.is_server:
            return cstate.client_life_id == msg.header.client_life_id and cstate.server_life_id == self.server_life_id
        else:
            return cstate.client_life_id == self.client_life_id and cstate.server_life_id == msg.header.server_life_id

    cdef ProtocolBaseMessage * _make_msg(self, ConnectionState *cstate, char msg_type, ProtocolStatus msg_status) nogil:
        """
        Creates a ProtocolBaseMessage for sending
        
        IMPORTANT:  Make sure that self.transport.send(...., no_copy=True), to avoid memory leaks
        
        :param cstate: 
        :param msg_type: 
        :param msg_status: 
        :return: 
        """
        cyassert(msg_type != 0) # Real char allowed

        cdef ProtocolBaseMessage *msg_out = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        msg_out.header.protocol_id = PROTOCOL_ID_BASE
        msg_out.header.msg_type = msg_type
        msg_out.header.server_life_id = cstate.server_life_id
        msg_out.header.client_life_id = cstate.client_life_id
        msg_out.status = msg_status
        return msg_out

    cdef ProtocolStatus _state_transition(self, ProtocolStatus conn_status, ProtocolStatus new_status) nogil:
        """
        Checking possible state transitions
        
        :param conn_status: 
        :param new_status: 
        :return:  next required connection state status 
        """
        if conn_status == ProtocolStatus.UHF_INACTIVE:
            if new_status == ProtocolStatus.UHF_CONNECTING:
                return ProtocolStatus.UHF_CONNECTING
            else:
                # Error only connecting allowed
                return ProtocolStatus.UHF_INACTIVE
        elif conn_status == ProtocolStatus.UHF_CONNECTING:
            if new_status == ProtocolStatus.UHF_INITIALIZING:
                # Allow initialization after connect
                return ProtocolStatus.UHF_INITIALIZING
            elif new_status == ProtocolStatus.UHF_ACTIVE:
                # Allow activate immediately after connect too
                return ProtocolStatus.UHF_ACTIVE
            else:
                return ProtocolStatus.UHF_INACTIVE
        elif conn_status == ProtocolStatus.UHF_INITIALIZING:
            if new_status == ProtocolStatus.UHF_INITIALIZING:
                # Allow multiple initialization requests
                return ProtocolStatus.UHF_INITIALIZING
            elif new_status == ProtocolStatus.UHF_ACTIVE:
                # Allow activating
                return ProtocolStatus.UHF_ACTIVE
            else:
                return ProtocolStatus.UHF_INACTIVE
        elif conn_status == ProtocolStatus.UHF_ACTIVE:
            if new_status == ProtocolStatus.UHF_ACTIVE:
                # Allow activating
                return ProtocolStatus.UHF_ACTIVE
            else:
                return ProtocolStatus.UHF_INACTIVE

        cyassert(0) # conn_status - Not implemented!
        return ProtocolStatus.UHF_INACTIVE




