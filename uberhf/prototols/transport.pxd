from uberhf.prototols.libzmq cimport *
from libc.stdint cimport uint16_t

cdef extern from "../include/uhfprotocols.h"  nogil:
    const uint16_t TRANSPORT_HDR_MGC
    const size_t TRANSPORT_SENDER_SIZE

    const int TRANSPORT_ERR_OK
    const int TRANSPORT_ERR_BAD_SIZE
    const int TRANSPORT_ERR_BAD_HEADER
    const int TRANSPORT_ERR_BAD_PARTSCOUNT
    const int TRANSPORT_ERR_SOCKET_CLOSED


ctypedef struct TransportHeader:
    uint16_t magic_number
    char sender_id[TRANSPORT_SENDER_SIZE]



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

    cdef readonly char * transport_topic
    cdef readonly int transport_topic_len

    cdef readonly int msg_received
    cdef readonly int msg_sent
    cdef readonly int msg_errors
    cdef readonly int last_error

    cdef int get_last_error(self)

    cdef char* get_last_error_str(self, int errnum)

    cdef int send(self, char *topic, void *data, size_t size, bint no_copy)

    cdef void receive_finalize(self, void *data)

    cdef void * receive(self, size_t *size)

    cdef void close(self)
