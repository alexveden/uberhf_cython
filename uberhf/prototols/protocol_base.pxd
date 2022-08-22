from uberhf.includes.uhfprotocols cimport ProtocolStatus, TRANSPORT_SENDER_SIZE
from .transport cimport Transport, TransportHeader
from uberhf.includes.hashmap cimport HashMap

ctypedef struct ConnectionState:
    # Keep sender_id as first item to allow HashMap seek by string
    char sender_id[TRANSPORT_SENDER_SIZE + 1]
    unsigned int server_life_id
    unsigned int client_life_id
    ProtocolStatus status
    long last_heartbeat_time_ns
    size_t msg_sent
    size_t msg_recvd
    size_t msg_errs


ctypedef struct ProtocolBaseMessage:
    TransportHeader header
    ProtocolStatus status


cdef class ProtocolBase:
    cdef bint is_server
    cdef Transport transport
    cdef HashMap connections
    cdef unsigned int server_life_id
    cdef unsigned int client_life_id

    cdef void initialize(self, bint is_server, int module_id, Transport transport)
    cdef ConnectionState * get_state(self, char * sender_id) nogil

    #
    # Client / server commands + event handlers
    #
    cdef int send_connect(self) nogil
    cdef int on_connect(self, ProtocolBaseMessage * msg) nogil

    cdef int send_activate(self) nogil
    cdef int on_activate(self, ProtocolBaseMessage * msg) nogil

    cdef int send_disconnect(self)
    cdef int on_disconnect(self, ProtocolBaseMessage * msg) nogil

    #
    # Private methods
    #
    cdef ProtocolBaseMessage * _make_msg(self, ConnectionState *cstate, char msg_type, ProtocolStatus msg_status) nogil
    cdef bint _check_life_id(self, ConnectionState *cstate, ProtocolBaseMessage *msg) nogil
    cdef ProtocolStatus _state_transition(sellf, ProtocolStatus conn_status, ProtocolStatus new_status) nogil
