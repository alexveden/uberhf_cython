from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE
from libc.stdint cimport uint16_t


ctypedef struct TransportHeader:
    uint16_t magic_number
    char sender_id[TRANSPORT_SENDER_SIZE + 1]
    char protocol_id
    char msg_type
    int my_id
    int foreign_id
    int request_id



cdef class Transport:
    """
    ZeroMQ universal transport layer
    """
    cdef void * context
    cdef void * socket
    cdef zmq_msg_t * last_msg_received_ptr
    cdef zmq_msg_t last_msg
    cdef void * last_data_received_ptr

    cdef readonly int socket_type

    cdef readonly char transport_id[TRANSPORT_SENDER_SIZE]
    cdef readonly int transport_id_len

    cdef readonly int msg_received
    cdef readonly int msg_sent
    cdef readonly int msg_errors
    cdef readonly int last_error

    cdef int get_last_error(self)
    cdef char* get_last_error_str(self, int errnum)

    cdef int send(self, char *topic, void *data, size_t size, bint no_copy)  nogil
    cdef int _send_set_error(self, int err_code, void * data) nogil

    cdef void * receive(self, size_t *size) nogil
    cdef void * _receive_set_error(self, int errcode, size_t *size, bint close_msg) nogil
    cdef void receive_finalize(self, void *data)  nogil

    cdef void close(self) nogil