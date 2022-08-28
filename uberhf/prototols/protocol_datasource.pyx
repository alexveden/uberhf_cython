from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport
from uberhf.includes.uhfprotocols cimport PROTOCOL_ID_DATASOURCE, ProtocolStatus, PROTOCOL_ERR_WRONG_ORDER, PROTOCOL_ERR_ARG_ERR, PROTOCOL_ERR_SRV_ERR
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE
from uberhf.prototols.protocol_base cimport ProtocolBase
from uberhf.prototols.messages cimport TransportHeader, ProtocolDSRegisterMessage, ProtocolDSQuoteMessage


# Set child protocol message types in lower case to avoid conflicts with BaseProtocol
DEF MSGT_REGISTER = b'r'
DEF MSGT_QUOTE = b'q'
DEF MSGT_IINFO = b'i'


cdef class ProtocolDataSource(ProtocolBase):
    def __cinit__(self, module_id, transport, source_client = None, feed_server = None, heartbeat_interval_sec=5):
        if source_client is None and feed_server is None:
            raise ValueError(f'You must set one of source_client or feed_server')
        elif source_client is not None and feed_server is not None:
            raise ValueError(f'Arguments are mutually exclusive: source_client, feed_server')
        cdef  bint is_server = 0
        #cybreakpoint(1)
        #breakpoint()
        if source_client is not None:
            is_server = 0
            self.source_client = source_client
            self.source_client.register_datasource_protocol(self)
        elif feed_server is not None:
            is_server = 1
            self.feed_server = feed_server
            self.feed_server.register_datasource_protocol(self)

        # Calling super() class in Cython must be by static
        ProtocolBase.protocol_initialize(self, PROTOCOL_ID_DATASOURCE, is_server, module_id, transport, heartbeat_interval_sec)


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
        # super() method just only does - self.send_activate()
        #return ProtocolBase.initialize_client(self, cstate)
        if self.is_server:
            self.feed_server.source_on_initialize(cstate.sender_id, cstate.client_life_id)
        else:
            self.source_client.source_on_initialize()

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
        if self.is_server:
            self.feed_server.source_on_activate(cstate.sender_id)
        else:
            self.source_client.source_on_activate()

    cdef void disconnect_client(self, ConnectionState * cstate) nogil:
        """
        Set internal connection state as disconnected, this method should also be overridden by child classes for additional logic

        This is client/server method!     

        :param cstate: 
        :return: 
        """
        # Calling super() method is mandatory!
        ProtocolBase.disconnect_client(self, cstate)

        if self.is_server:
            self.feed_server.source_on_disconnect(cstate.sender_id)
        else:
            self.source_client.source_on_disconnect()

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil:
        """
        Client / server dispatcher of datasource messages

        :param msg: 
        :param msg_size: 
        :return: 
        """
        cdef TransportHeader * hdr = <TransportHeader *> msg
        cdef int rc = 0

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            return rc

        # In order from the most frequent to less frequent
        if hdr.msg_type == MSGT_QUOTE:
            if msg_size != sizeof(ProtocolDSQuoteMessage):
                return PROTOCOL_ERR_SIZE
            self.feed_server.source_on_quote(<ProtocolDSQuoteMessage*>msg)
            return 1
        # elif hdr.msg_type == MSGT_IINFO:
        #     cyassert(0)
        if hdr.msg_type == MSGT_REGISTER:
            # Source initialization request/reply
            if msg_size != sizeof(ProtocolDSRegisterMessage):
                return PROTOCOL_ERR_SIZE
            rc = self.on_register_instrument(<ProtocolDSRegisterMessage *> msg)
            cyassert(rc != 0)
        else:
            rc = ProtocolBase.on_process_new_message(self, msg, msg_size)
            cyassert(rc != 0)

        return rc

    #
    #  PROTOCOL SPECIFIC METHODS
    #
    cdef int send_register_instrument(self, char * v2_ticker, uint64_t instrument_id, InstrumentInfo * iinfo) nogil:
        """
        Data source client send registration request for all v2 tickers it's going to source
        
        :param v2_ticker: full qualified v2 ticker  
        :param instrument_id: unique ticker ID for this source
        :param iinfo: instrument info specification    
        :return: >0 if success, or error
        """
        cyassert( self.is_server == 0) # Only clients allowed

        cdef ConnectionState * cstate = self.get_state(b'')

        if cstate.status != ProtocolStatus.UHF_INITIALIZING:
            return PROTOCOL_ERR_WRONG_ORDER
        if v2_ticker == NULL or strlen(v2_ticker) > V2_TICKER_MAX_LEN-1:
            return PROTOCOL_ERR_ARG_ERR
        if instrument_id == 0:
            return PROTOCOL_ERR_ARG_ERR

        cdef ProtocolDSRegisterMessage *msg_out = <ProtocolDSRegisterMessage *> malloc(sizeof(ProtocolDSRegisterMessage))
        msg_out.header.protocol_id = self.protocol_id
        msg_out.header.msg_type = MSGT_REGISTER
        msg_out.header.server_life_id = cstate.server_life_id
        msg_out.header.client_life_id = cstate.client_life_id

        strlcpy(msg_out.v2_ticker, v2_ticker, V2_TICKER_MAX_LEN)
        msg_out.instrument_id = instrument_id
        # These reserved for server reply
        msg_out.error_code = 0
        msg_out.instrument_index = -1
        msg_out.iinfo = iinfo[0]
        return self.transport.send(NULL, msg_out, sizeof(ProtocolDSRegisterMessage), no_copy=1)

    cdef int send_new_quote(self, ProtocolDSQuoteMessage* msg, int send_no_copy) nogil:
        """
        Data source sends new quotes to the UHFeed server
        
        :param msg: 
        :param send_no_copy: 
        :return: 
        """
        cyassert(self.is_server == 0)  # Only clients allowed
        cyassert(msg != NULL)
        cyassert(msg.header.msg_type == MSGT_QUOTE)

        return self.transport.send(NULL, msg, sizeof(ProtocolDSQuoteMessage), no_copy=send_no_copy)

    cdef int on_register_instrument(self, ProtocolDSRegisterMessage *msg) nogil:
        """
        Client / server handler of new registration requests
        
        :param msg: 
        :return: 
        """
        cyassert(msg != NULL)

        cdef int rc = 0
        cdef ProtocolDSRegisterMessage *msg_out

        if self.is_server:
            # Pass request to the core and redirect the reply to the client
            rc = self.feed_server.source_on_register_instrument(msg.header.sender_id, msg.v2_ticker, msg.instrument_id, &msg.iinfo)

            msg_out = <ProtocolDSRegisterMessage *> malloc(sizeof(ProtocolDSRegisterMessage))
            msg_out.header.protocol_id = self.protocol_id
            msg_out.header.msg_type = MSGT_REGISTER
            msg_out.header.server_life_id = msg.header.server_life_id
            msg_out.header.client_life_id = msg.header.client_life_id

            strlcpy(msg_out.v2_ticker, msg.v2_ticker, V2_TICKER_MAX_LEN)
            msg_out.instrument_id = msg.instrument_id

            if rc < 0:
                # Error
                msg_out.error_code = rc
                msg_out.instrument_index = -1
                memset(&msg_out.iinfo, 0, sizeof(InstrumentInfo))
            else:
                msg_out.error_code = 0  # All good, no error
                msg_out.instrument_index = rc
                msg_out.iinfo = msg.iinfo

            return self.transport.send(msg.header.sender_id, msg_out, sizeof(ProtocolDSRegisterMessage), no_copy=True)
        else:
            # Processing confirmation from the server
            rc = self.source_client.source_on_register_instrument(msg.v2_ticker, msg.instrument_id, msg.error_code, msg.instrument_index)
            if rc <= 0:
                # Force returning error even if client returns 0, to not confuse on_process_new_message()
                return PROTOCOL_ERR_CLI_ERR
            else:
                return rc










