from uberhf.prototols.libzmq cimport *

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
    cdef readonly char transport_id

    cdef int get_last_error(self)

    cdef char* get_last_error_str(self, int errnum)

    cdef int send(self, void *data, size_t size, bint no_copy)

    cdef void receive_finalize(self, void *data)

    cdef void * receive(self, size_t *size)

    cdef void close(self)
