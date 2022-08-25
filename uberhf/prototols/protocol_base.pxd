from uberhf.includes.uhfprotocols cimport ProtocolStatus, TRANSPORT_SENDER_SIZE
from .transport cimport Transport, TransportHeader
from uberhf.includes.hashmap cimport HashMap

ctypedef struct ConnectionState:
    # Keep sender_id as first item to allow HashMap seek by string
    char sender_id[TRANSPORT_SENDER_SIZE + 1]
    unsigned int server_life_id
    unsigned int client_life_id
    ProtocolStatus status
    long last_msg_time_ns
    size_t msg_sent
    size_t msg_recvd
    size_t msg_errs
    size_t n_heartbeats


ctypedef struct ProtocolBaseMessage:
    TransportHeader header
    ProtocolStatus status


cdef class ProtocolBase:
    cdef bint is_server
    cdef Transport transport
    cdef HashMap connections
    cdef unsigned int server_life_id
    cdef unsigned int client_life_id
    cdef double heartbeat_interval_sec
    cdef char protocol_id

    cdef void protocol_initialize(self, char protocol_id, bint is_server, int module_id, Transport transport, double heartbeat_interval_sec)
    cdef ConnectionState * get_state(self, char * sender_id) nogil

    #
    # Client / server commands + event handlers
    #
    cdef int send_connect(self) nogil
    cdef int on_connect(self, ProtocolBaseMessage * msg) nogil

    cdef int send_initialize(self) nogil
    cdef int on_initialize(self, ProtocolBaseMessage * msg) nogil

    cdef int send_activate(self) nogil
    cdef int on_activate(self, ProtocolBaseMessage * msg) nogil

    cdef int send_disconnect(self) nogil
    cdef int on_disconnect(self, ProtocolBaseMessage * msg) nogil

    cdef int send_heartbeat(self) nogil
    cdef int on_heartbeat(self, ProtocolBaseMessage * msg) nogil

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
    cdef int heartbeat(self, long dtnow) nogil

    #
    # Methods for child classes
    #
    cdef void initialize_client(self, ConnectionState * cstate) nogil
    cdef void activate_client(self, ConnectionState * cstate) nogil
    cdef void disconnect_client(self, ConnectionState * cstate) nogil

    #
    # Private methods
    #
    cdef int _send_command(self, ConnectionState *cstate, ProtocolStatus new_status, char msg_type) nogil
    cdef int _on_msg_reply(self,
                           ConnectionState * cstate,
                           ProtocolBaseMessage *msg,
                           ProtocolStatus expected_status,
                           bint check_life_id,
                           char server_reply_msg_type,
                           ) nogil
    cdef ProtocolBaseMessage * _make_msg(self, ConnectionState *cstate, char msg_type, ProtocolStatus msg_status) nogil
    cdef bint _check_life_id(self, ConnectionState *cstate, ProtocolBaseMessage *msg) nogil
    cdef ProtocolStatus _state_transition(self, ProtocolStatus conn_status, ProtocolStatus new_status) nogil
