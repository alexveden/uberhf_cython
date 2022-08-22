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
    cdef ConnectionState * get_state(self, char * sender_id)
    cdef int connect(self)
    cdef int on_connect(self, ProtocolBaseMessage * msg)
