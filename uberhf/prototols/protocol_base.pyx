from uberhf.includes.uhfprotocols cimport ProtocolStatus, TRANSPORT_SENDER_SIZE, PROTOCOL_ID_BASE, PROTOCOL_ERR_WRONG_ORDER, \
                                          PROTOCOL_ERR_LIFE_ID, PROTOCOL_ERR_WRONG_TYPE, PROTOCOL_ERR_CLI_TIMEO, PROTOCOL_ERR_SRV_TIMEO, \
                                          TRANSPORT_HDR_MGC
from .transport cimport Transport
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from libc.stdlib cimport malloc, free
from libc.string cimport strlen
from uberhf.includes.hashmap cimport HashMap
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id, timedelta_ns, TIMEDELTA_SEC
from uberhf.prototols.libzmq cimport *



DEF MSGT_CONNECT = b'C'
DEF MSGT_INITIALIZE = b'I'
DEF MSGT_ACTIVATE = b'A'
DEF MSGT_DISCONNECT = b'D'
DEF MSGT_HEARTBEAT = b'H'


cdef class ProtocolBase:
    # Skipping __cinit__ - to allow child classes to have arbitrary constructor arguments!
    #def __cinit__(self, is_server, module_id, transport, heartbeat_interval_sec=5):
    #    self.protocol_initialize(PROTOCOL_ID_BASE, is_server, module_id, transport, heartbeat_interval_sec)

    cdef void protocol_initialize(self, char protocol_id, bint is_server, int module_id, Transport transport, double heartbeat_interval_sec):
        """
        Basic constructor method to make sure the class inheritance work
        
        :param protocol_id: unique protocol ID
        :param is_server: 1 - protocol instance is a server, 0 - is client
        :param module_id: unique module id between (0;40)        
        :param transport: Transport instance, must be ZMQ_ROUTER for server, ZMQ_DEALER for client!
        :param heartbeat_interval_sec: heartbeat interval, in seconds (fractional allowed too!)
        :return: 
        """
        assert module_id >0 and module_id <= 40, 'Module ID must be >0 and <= 40'
        assert heartbeat_interval_sec > 0 and heartbeat_interval_sec < 300, 'heartbeat_interval_sec expected between (0, 300) seconds'

        self.protocol_id = protocol_id
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
        self.heartbeat_interval_sec = heartbeat_interval_sec

    cdef ConnectionState * get_state(self, char * sender_id) nogil:
        """
        Get client/server connection state, or creates new instance if not found        
        
        :param sender_id: when called as client `sender_id` must be (b""), for server as msg.header.sender_id        
        :return: connection state pointer
        """
        cyassert (sender_id != NULL)
        if self.is_server:
            cyassert(strlen(sender_id) > 0)
        else:
            cyassert(strlen(sender_id) == 0)

        cdef ConnectionState* cstate = <ConnectionState*> self.connections.get(sender_id)
        if cstate == NULL:
            # Now initialized state!
            state = <ConnectionState *> malloc(sizeof(ConnectionState))
            strlcpy(state.sender_id, sender_id, TRANSPORT_SENDER_SIZE)
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
    #  Base protocol commands
    #
    cdef int send_connect(self) nogil:
        """
        Client connection to the server
        
        :return: 
        """
        cyassert(self.is_server == 0) # Only client can connect to the server!

        cdef ConnectionState * cstate = self.get_state(b'')
        cyassert(cstate.status == ProtocolStatus.UHF_INACTIVE)

        # Set this to avoid perpetual connection trials at heartbeat()
        cstate.last_msg_time_ns = datetime_nsnow()
        return self._send_command(cstate, ProtocolStatus.UHF_CONNECTING, MSGT_CONNECT)

    cdef int send_initialize(self) nogil:
        """
        Initialize protocol (typically exchanging internal states)

        :return: 
        """
        cyassert(self.is_server == 0)  # Only client can connect to the server!

        cdef ConnectionState * cstate = self.get_state(b'')
        return self._send_command(cstate, ProtocolStatus.UHF_INITIALIZING, MSGT_INITIALIZE)

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
        self.disconnect_client(cstate)
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
        cdef int rc = 0
        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
            return self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_CONNECTING, 0, MSGT_CONNECT)
        else:
            cstate = self.get_state(b'')
            rc = self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_CONNECTING, 0, MSGT_CONNECT)
            if rc > 0:
                return self.send_initialize()
            else:
                return rc

    cdef int on_initialize(self, ProtocolBaseMessage * msg) nogil:
        """
        Client / server `initialize` event handler

        :param msg: 
        :return: 
        """
        #cybreakpoint(1)
        cdef ConnectionState * cstate

        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        cdef int rc = self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_INITIALIZING, 1, MSGT_INITIALIZE)
        if rc > 0:
            self.initialize_client(cstate)
        return rc

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

        cdef int rc = self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_ACTIVE, 1, MSGT_ACTIVATE)
        if rc > 0:
            self.activate_client(cstate)
        return rc

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
            self.disconnect_client(cstate)
        else:
            return PROTOCOL_ERR_WRONG_ORDER

        return self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_INACTIVE, 0, 0)


    cdef int on_heartbeat(self, ProtocolBaseMessage * msg) nogil:
        """
        A heartbeat message for maintaining connection granularity
        
        :param msg: 
        :return: 
        """
        cdef ConnectionState * cstate
        if self.is_server:
            cstate = self.get_state(msg.header.sender_id)
        else:
            cstate = self.get_state(b'')

        cdef int rc = self._on_msg_reply(cstate, msg, ProtocolStatus.UHF_ACTIVE, 1, MSGT_HEARTBEAT)
        if rc > 0:
            cstate.n_heartbeats += 1
        return rc

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil:
        """
        Generic protocol message processor,

        supports only ProtocolBaseMessage

        :param msg: generic message 
        :param msg_size: msg size
        :return: 0 if protocol didn't match, or return code of the message handler (>0 ok, < 0 error) 
        """
        cdef int rc = 0
        cdef ProtocolBaseMessage * proto_msg = <ProtocolBaseMessage *> msg

        if msg_size != sizeof(ProtocolBaseMessage) or proto_msg.header.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return rc

        if proto_msg.header.msg_type == MSGT_HEARTBEAT:
            rc = self.on_heartbeat(proto_msg)
            cyassert(rc != 0)
            return rc
        elif proto_msg.header.msg_type == MSGT_CONNECT:
            rc = self.on_connect(proto_msg)
            cyassert(rc != 0)
            return rc
        elif proto_msg.header.msg_type == MSGT_INITIALIZE:
            rc = self.on_initialize(proto_msg)
            cyassert(rc != 0)
            return rc
        elif proto_msg.header.msg_type == MSGT_ACTIVATE:
            rc = self.on_activate(proto_msg)
            cyassert(rc != 0)
            return rc
        elif proto_msg.header.msg_type == MSGT_DISCONNECT:
            rc = self.on_disconnect(proto_msg)
            cyassert(rc != 0)
            return rc
        else:
            return PROTOCOL_ERR_WRONG_TYPE
    #
    # Prototype methods for overriding in child protocol classes
    #
    cdef void initialize_client(self, ConnectionState * cstate) nogil:
        """
        Initialization of the new connection
        
        - server gets this command when the client requests: send_initialize()
        - client gets this command when the server reply on_initialize()
        
        So server can set its internal state of the client, and the client can begin initialization sequence or just send_activate()
        
        This is client/server method!      
          
        :param cstate: 
        :return: 
        """
        if self.is_server:
            # Just confirms client initialization
            return
        else:
            # By default let's send activate command
            self.send_activate()

    cdef void activate_client(self, ConnectionState * cstate) nogil:
        """
        Activation of the initialized connection
        
         - server gets this command when the client requests: send_activate()
        - client gets this command when the server reply on_activate()
        
        So server can set its internal state of the client, and the protocol goes into active state
        
        This is client/server method!     
        
        :param cstate: 
        :return: 
        """
        return

    cdef void disconnect_client(self, ConnectionState * cstate) nogil:
        """
        Set internal connection state as disconnected, this method should also be overridden by child classes for additional logic
        
        This is client/server method!     
        
        :param cstate: 
        :return: 
        """
        cstate.status = ProtocolStatus.UHF_INACTIVE
        if self.is_server:
            cstate.client_life_id = 0
        else:
            cstate.server_life_id = 0
            self.server_life_id = 0

    cdef int heartbeat(self, long dtnow) nogil:
        """
        This method intended for call in ZMQ poller loops of the main application, it has to be called with some timeout interval 
        (say 100-200 millisec), to avoid CPU overload        
        :param dtnow: current time, datetime_nsnow()
        :return: positive or zero when ok, negative is error
        """
        cdef size_t i = 0
        cdef void * hm_data = NULL
        cdef ConnectionState * cstate = NULL
        cdef int rc = 0
        cdef double t_delta

        if self.is_server:

            # Check if clients are connected
            while self.connections.iter(&i, &hm_data):
                cstate = <ConnectionState *> hm_data
                t_delta = timedelta_ns(dtnow, cstate.last_msg_time_ns, TIMEDELTA_SEC)
                if cstate.status != ProtocolStatus.UHF_INACTIVE:
                    if t_delta >= self.heartbeat_interval_sec * 3:
                        # Client failed to send any messages of heartbeats
                        self.disconnect_client(cstate)
                        rc = PROTOCOL_ERR_CLI_TIMEO
            return rc
        else:
            #
            # Client!
            #
            cstate = self.get_state(b'')
            t_delta = timedelta_ns(dtnow, cstate.last_msg_time_ns, TIMEDELTA_SEC)

            if cstate.status == ProtocolStatus.UHF_INACTIVE:
                if t_delta >= self.heartbeat_interval_sec * 3:
                    return self.send_connect()
                else:
                    return 0
            else:
                if t_delta >= self.heartbeat_interval_sec * 3:
                    # Probably something went wrong, disconnecting, to be able to connect later
                    self.send_disconnect()
                    return PROTOCOL_ERR_SRV_TIMEO
                elif t_delta >= self.heartbeat_interval_sec:
                    return self.send_heartbeat()
                else:
                    return 0

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
                    self.server_life_id = 0
                else:
                    cstate.server_life_id = msg.header.server_life_id
                    self.server_life_id = msg.header.server_life_id
                return 1
        else:
            # Error in state transition
            cyassert(next_state == ProtocolStatus.UHF_INACTIVE)
            cstate.status = next_state
            cstate.msg_errs += 1
            if self.is_server:
                cstate.client_life_id = 0
            else:
                cstate.server_life_id = 0
                self.server_life_id = 0

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
        msg_out.header.protocol_id = self.protocol_id
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


