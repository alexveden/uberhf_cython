from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
from uberhf.prototols.libzmq cimport *
from zmq.backend.cython.context cimport Context
from zmq.error import ZMQError

cdef extern from "assert.h":
    # Replacing name to avoid conflict with python assert keyword!
    void cassert "assert"(bint)

cdef void _zmq_free_data_callback(void *data, void *hint):
    free(data)

cdef class Transport:
    cdef void * context
    cdef void * socket
    cdef zmq_msg_t last_message
    cdef readonly int socket_type


    def __cinit__(self, zmq_context, socket_endpoint, socket_type):
        self.context = <void*>zmq_context.handle
        self.last_msg_received_ptr = NULL

        self.socket = zmq_socket(self.context, socket_type)
        if self.socket == NULL:
            raise ZMQError()

        cdef int result = 0
        if socket_type in [ZMQ_REP, ZMQ_PUB]:
            result = zmq_bind(self.socket, socket_endpoint.decode())
        elif socket_type == [ZMQ_REQ, ZMQ_SUB]:
            result = zmq_connect(self.socket, socket_endpoint.decode())
        else:
            raise NotImplementedError(f'Unsupported socket type: {socket_type}')
        if result != 0:
            raise ZMQError()


    cdef int get_last_error(self):
        return zmq_errno()

    cdef char* get_last_error_str(self, int errnum):
        return zmq_strerror(errnum)

    cdef int send(self, void *data, size_t size, bint no_copy):
        cdef zmq_msg_t msg
        cdef int rc

        if no_copy:
            rc = zmq_msg_init_data (&msg, data, size, <zmq_free_fn*>_zmq_free_data_callback, NULL)
        else:
            rc = zmq_msg_init_size (&msg, size)
            # Using zmq_msg_data(&msg) as in example: http://api.zeromq.org/master:zmq-msg-send
            memcpy(zmq_msg_data(&msg), data, size)

        # Data initialization failure (out of mem?)
        cassert(rc == 0)

        rc = zmq_msg_send(&msg, self.socket, 0)
        # Sending failure
        cassert(rc == size)
        return rc

    cdef void receive_finalize(self):
        # Check if not already finalized
        cassert(self.last_msg_received_ptr != NULL)

        cdef int rc = zmq_msg_close(self.last_msg_received_ptr)
        # The zmq_msg_close() function shall return zero if successful.
        # Otherwise it shall return -1 and set errno to one of the values defined below.
        cassert(rc == 0)

    cdef void * receive(self, size_t *size):
        # Make sure that previous call called receive_finalize()
        #    or protocol calls req_finalize() when it's done!!!
        cassert(self.last_msg_received_ptr == NULL)

        cdef zmq_msg_t msg
        cdef int rc = zmq_msg_init(&msg)
        cassert(rc == 0)

        rc = zmq_msg_recv(&msg, self.socket, 0)
        cassert(rc != -1)

        size[0] = zmq_msg_size(&msg)

        return zmq_msg_data(&msg)

    def __dealloc__(self):
        if self.socket != NULL:
            # TODO: consider setting LINGER
            zmq_close(self.socket)


cdef class Protocol:
    cdef Transport reqrep_transport

    def __cinit__(self, Transport reqrep_transport):
        self.reqrep_transport = reqrep_transport

    cdef void* req_rep(self, void *out_request, size_t out_size, size_t *size_recv):
        """
        Client function for getting REQ/REP in blocking mode
        
        IMPORTANT: you must copy / process the result of this function and always call self.req_finalize()
                   when done with it to avoid memory leaks     
           
        :param out_request: request data
        :param out_size:  request size
        :param size_recv: pointer to received data size
        :return: malloc'ed data buffer
        """
        # This only allowed for clients!
        cassert(self.reqrep_transport.socket_type == ZMQ_REQ)

        self.reqrep_transport.send(out_request, out_size, 1)

        cdef void *tmp_data = self.reqrep_transport.receive(size_recv)

        # MUST BE CALLED BY CHILD CLASS to free ZeroMQ memory buffer
        # self.transport.receive_finalize()

        # You must free later!
        return tmp_data

    cdef void req_finalize(self):
        """
        Finalize dynamically allocated buffer of last received REQ        
        :return: 
        """
        self.reqrep_transport.receive_finalize()

    cdef int rep(self, void *out_reply, size_t out_size, bint no_copy):
        """
        Generic server reply via REP/REQ transport
        
        :param out_reply: generic data buffer 
        :param out_size:  data size
        :param no_copy: if 1 then out_reply will be freed down the road, don't reuse this data or free(out_reply)!!!
                        if 0 the data will be fully copied (extra overhead)
        :return: 
        """
        # This only allowed for server!
        cassert(self.reqrep_transport.socket_type == ZMQ_REP)

        return self.reqrep_transport.send(out_reply, out_size, no_copy)

    cdef void* on_req(self, size_t *size_recv):
        """
        Received generic request from client.
        
        This function typically should be used with zmq poller
        
        IMPORTANT: you must copy / process the incoming data and always call self.req_finalize()
                   when done with it to avoid memory leaks         
        
        :param size_recv: size of received data 
         
        :return: void* buffer for received data     
        """
        # This only allowed for server!
        cassert(self.reqrep_transport.socket_type == ZMQ_REP)

        return self.reqrep_transport.receive(size_recv)


ctypedef struct TickerIdx:
    char ticker[4]
    int idx_position

cdef class FeedProtocol(Protocol):
    cdef TickerIdx req_connect(self, char * ticker):
        cdef size_t recv_size = 0
        cdef void *data = self.req_rep(ticker, 4, &recv_size)
        cdef TickerIdx ti;
        cdef TickerIdx *pti = <TickerIdx*> data;

        ti.idx_position = pti.idx_position

        self.req_finalize()
        return ti

cpdef main():
    zmq_ctx = Context()

    t = Transport(zmq_ctx, None)

    p = Protocol(t)


    p.rep(b'abcd', 4, no_copy=True)

    cdef size_t recv_size = 0
    cdef void * data = p.req_rep(b'gfdas', 4, &recv_size)