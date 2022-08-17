from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t


cdef extern from "../include/safestr.h"  nogil:
    size_t strlcpy(char *dst, const char *src, size_t dsize)

cdef extern from "assert.h":
    # Replacing name to avoid conflict with python assert keyword!
    void cassert "assert"(bint)

cdef void _zmq_free_data_callback(void *data, void *hint):
    # This is called by ZeroMQ internally, when sending with no_copy=True
    free(data)


cdef class Transport:
    """
    ZeroMQ universal transport layer
    """
    def __init__(self, zmq_context_ptr, socket_endpoint, socket_type, transport_topic):
        """
        Initialize ZMQ Transport

        :param zmq_context:
        :param socket_endpoint:
        :param socket_type:
        """
        pass

    def __cinit__(self, zmq_context_ptr, socket_endpoint, socket_type, transport_topic):
        self.context = <void*>(<uint64_t>zmq_context_ptr)
        self.last_msg_received_ptr = NULL
        self.msg_errors = 0
        self.msg_sent = 0
        self.msg_received = 0
        self.last_error = TRANSPORT_ERR_OK

        assert len(transport_topic) > 0, f'transport_topic must be a single string'
        assert len(transport_topic) <= TRANSPORT_SENDER_SIZE-1, f'transport_topic must be short, <= {TRANSPORT_SENDER_SIZE}'
        self.transport_topic = <char*>transport_topic
        self.transport_topic_len = len(transport_topic)

        cdef int linger = 2000  # 2 seconds
        cdef int router_mandatory = 1

        printf('cotext addr: %u\n', self.context)

        self.socket_type = socket_type
        self.socket = zmq_socket(self.context, self.socket_type)

        if self.socket == NULL:
            err = self.get_last_error()
            print(f'ZMQ Err: {err}: {self.get_last_error_str(err)}')
            raise ZMQError()

        cdef int result = 0
        printf('Binding\n')

        if self.socket_type in [ZMQ_PUB, ZMQ_ROUTER]:
            if self.socket_type == ZMQ_ROUTER:
                zmq_setsockopt(self.socket, ZMQ_ROUTER_MANDATORY, &router_mandatory, sizeof(int))

            result = zmq_bind(self.socket, <char*>socket_endpoint)
        elif self.socket_type in [ZMQ_SUB, ZMQ_DEALER]:
            zmq_setsockopt(self.socket, ZMQ_LINGER, &linger, sizeof(int))

            if self.socket_type == ZMQ_DEALER:
                zmq_setsockopt(self.socket, ZMQ_ROUTING_ID, self.transport_topic, self.transport_topic_len)
                pass

            result = zmq_connect(self.socket, <char*>socket_endpoint)
        else:
            raise NotImplementedError(f'Unsupported socket type: {self.socket_type}')
        if result != 0:
            raise ZMQError()

    cdef int get_last_error(self):
        if self.last_error > 64000:
            return self.last_error

        return zmq_errno()

    cdef char* get_last_error_str(self, int errnum):
        if self.last_error > 64000:
            if self.last_error == TRANSPORT_ERR_BAD_SIZE:
                return 'Transport data has less than TransportHeader size'
            if self.last_error == TRANSPORT_ERR_BAD_HEADER:
                return 'Transport invalid header signature'
            if self.last_error == TRANSPORT_ERR_BAD_PARTSCOUNT:
                return 'Transport unexpected multipart count'
            if self.last_error == TRANSPORT_ERR_SOCKET_CLOSED:
                return 'Socket already closed'

        return zmq_strerror(errnum)

    cdef int send(self, char *topic, void *data, size_t size, bint no_copy):
        self.last_error = TRANSPORT_ERR_OK
        cdef int rc

        cassert(size > sizeof(TransportHeader))

        # Sending transport ID for routing
        if topic != NULL:
            rc = zmq_send(self.socket, topic, strlen(topic), ZMQ_SNDMORE)

            # Expected to send 1 byte here as a header
            cassert(rc == self.transport_topic_len)

        cdef zmq_msg_t msg
        cdef TransportHeader* hdr = <TransportHeader*>data
        hdr.magic_number = TRANSPORT_HDR_MGC
        strlcpy(hdr.sender_id, self.transport_topic, TRANSPORT_SENDER_SIZE)

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

        self.msg_sent += 1

        return rc

    cdef void receive_finalize(self, void *data):
        # Check if not already finalized
        cassert(self.last_msg_received_ptr != NULL)

        # Check if the finalized data pointer address equal
        cassert(self.last_data_received_ptr == data)

        cdef int rc = zmq_msg_close(&self.last_msg)
        # The zmq_msg_close() function shall return zero if successful.
        # Otherwise it shall return -1 and set errno to one of the values defined below.
        cassert(rc == 0)

        # Clean up
        self.last_msg_received_ptr = NULL
        self.last_data_received_ptr = NULL

    cdef void * receive(self, size_t *size):
        # Make sure that previous call called receive_finalize()
        #    or protocol calls req_finalize() when it's done!!!
        self.last_error = TRANSPORT_ERR_OK

        cassert(self.last_msg_received_ptr == NULL)

        cdef int rc = 0
        cdef int msg_part = 0
        cdef void * data
        cdef TransportHeader* hdr

        while True:
            rc = zmq_msg_init(&self.last_msg)
            cassert(rc == 0)

            rc = zmq_msg_recv(&self.last_msg, self.socket, 0)
            cassert(rc != -1)

            size[0] = zmq_msg_size(&self.last_msg)

            msg_part += 1

            if not zmq_msg_more(&self.last_msg):
                #printf('Last Frame: bytes %d\n', size[0])
                break # Last frame arrived
            else:
                #printf('Multipart message: %s\n', <char*> zmq_msg_data(&self.last_msg))
                rc = zmq_msg_close(&self.last_msg)
                cassert(rc == 0)

        if msg_part != 2:
            # TODO: decide how to deal with unexpected multipart
            cassert(msg_part == 2)
            data = NULL
            self.last_error = TRANSPORT_ERR_BAD_PARTSCOUNT
        else:
            data = zmq_msg_data(&self.last_msg)
            if size[0] < sizeof(TransportHeader):
                data = NULL
                self.last_error = TRANSPORT_ERR_BAD_SIZE
            else:
                hdr = <TransportHeader*>data
                if hdr.magic_number != TRANSPORT_HDR_MGC:
                    data = NULL
                    self.last_error = TRANSPORT_ERR_BAD_HEADER

        if data == NULL:
            self.msg_errors += 1
            rc = zmq_msg_close(&self.last_msg)
            cassert(rc == 0)
            self.last_data_received_ptr = NULL
            self.last_msg_received_ptr = NULL
        else:
            self.last_msg_received_ptr = &self.last_msg
            self.last_data_received_ptr = data
            self.msg_received += 1

        return self.last_data_received_ptr

    cdef void close(self):
        cdef int timeout = 0  # 2 seconds
        self.last_error = TRANSPORT_ERR_SOCKET_CLOSED

        if self.socket != NULL:
            printf('Closing socket\n')
            zmq_setsockopt(self.socket, ZMQ_LINGER, &timeout, sizeof(int))
            zmq_close(self.socket)
            self.socket = NULL

    def __dealloc__(self):
        cdef int timeout = 0  # 2 seconds

        if self.socket != NULL:
            printf('__dealloc__\n')
            zmq_setsockopt(self.socket, ZMQ_LINGER, &timeout, sizeof(int))
            zmq_close(self.socket)

