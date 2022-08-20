from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from uberhf.includes.strutils cimport strlcpy
from uberhf.includes.asserts cimport cyassert
from uberhf.includes.uhfprotocols cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t


cdef void _zmq_free_data_callback(void *data, void *hint): # pragma: no cover
    # This is called by ZeroMQ internally, when sending with no_copy=True
    #
    #   The `void *data` must be malloc'ed previously somewhere in the user-space code!
    #
    # IMPORTANT: if you get bad address assertion/error in this place this means that you possibly
    #            tried no_copy=True send on local variable address
    #
    free(data)


cdef class Transport:
    """
    ZeroMQ universal transport layer
    """
    # def __init__(self, zmq_context_ptr, socket_endpoint, socket_type, transport_id):
    #     """
    #     Initialize ZMQ Transport
    #
    #     :param zmq_context_ptr:
    #     :param socket_endpoint:
    #     :param socket_type:
    #     """
    #     pass
    #
    def __cinit__(self, zmq_context_ptr, socket_endpoint, socket_type, transport_id, socket_timeout=100, sub_topic=None):
        """
        Initialized ZeroMQ transport

         Examples of usage:
                cdef void * ctx = zmq_ctx_new()
                Transport(<uint64_t>ctx, b'tcp://localhost:7100', ZMQ_DEALER, b'CLI')

                ctx = Context()
                transport = Transport(<uint64_t>ctx.underlying, b'tcp://*:7100', ZMQ_PUB, b'SRV')

        :param zmq_context_ptr: uint64_t or long long number, which will is void* pointer to the zmq_context()
        :param socket_endpoint: const char* string or python byte string
        :param socket_type: ZMQ_PUB / ZMQ_SUB / ZMQ_ROUTER / ZMQ_DEALER
        :param transport_id: unique transport ID byte-string (5 MAX)
        :param socket_timeout: socket sync receive/send timeout (this typically does not affect zmq_poll!)
        :param sub_topic:
        :return:
        """
        self.context = <void*>(<uint64_t>zmq_context_ptr)
        assert self.context != NULL, f'Context got NULL pointer!'

        self.last_msg_received_ptr = NULL
        self.msg_errors = 0
        self.msg_sent = 0
        self.msg_received = 0
        self.last_error = TRANSPORT_ERR_OK

        assert len(transport_id) > 0, f'transport_topic must be a byte string of [1; {TRANSPORT_SENDER_SIZE}] size'
        assert len(transport_id) <= TRANSPORT_SENDER_SIZE, f'transport_topic must be short, <= {TRANSPORT_SENDER_SIZE}'
        self.transport_id_len = len(transport_id)
        memcpy(self.transport_id, <char*>transport_id, self.transport_id_len)

        cdef int linger = 2000                      # in milliseconds
        cdef int rcv_timeout = <int>socket_timeout    # in milliseconds
        cdef int router_mandatory = 1

        self.socket_type = socket_type
        self.socket = zmq_socket(self.context, self.socket_type)

        if self.socket == NULL:
            raise ZMQError()

        cdef int result = 0
        if self.socket_type in [ZMQ_PUB, ZMQ_ROUTER]:
            if self.socket_type == ZMQ_ROUTER:
                zmq_setsockopt(self.socket, ZMQ_ROUTER_MANDATORY, &router_mandatory, sizeof(int))
                #zmq_setsockopt(self.socket, ZMQ_RCVTIMEO, &rcv_timeout, sizeof(int))
                zmq_setsockopt(self.socket, ZMQ_SNDTIMEO, &rcv_timeout, sizeof(int))
            elif self.socket_type == ZMQ_PUB:
                if sub_topic is not None:
                    raise ValueError(f'sub_topic applicable only for clients, ZMQ_SUB sockets')

            # Binding as server
            result = zmq_bind(self.socket, <char*>socket_endpoint)
        elif self.socket_type in [ZMQ_SUB, ZMQ_DEALER]:
            if self.socket_type == ZMQ_DEALER:
                zmq_setsockopt(self.socket, ZMQ_ROUTING_ID, self.transport_id, self.transport_id_len)

                zmq_setsockopt(self.socket, ZMQ_SNDTIMEO, &rcv_timeout, sizeof(int))
            elif self.socket_type == ZMQ_SUB:
                if sub_topic is None:
                    zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, b'', 0)
                elif isinstance(sub_topic, list):
                    for s in sub_topic:
                        zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, <char*>s, len(s))
                else:
                    zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, <char*>sub_topic, len(sub_topic))

            zmq_setsockopt(self.socket, ZMQ_RCVTIMEO, &rcv_timeout, sizeof(int))

            # Connecting as client
            result = zmq_connect(self.socket, <char*>socket_endpoint)
        else:
            raise NotImplementedError(f'Unsupported socket type: {self.socket_type}')

        if result != 0:
            raise ZMQError()

    cdef int get_last_error(self):
        """
        Get last error code of transport (if specific) or ZMQ 
        :return: 
        """
        if self.last_error < -64000:
            return self.last_error

        return zmq_errno()

    cdef char* get_last_error_str(self, int errnum):
        """
        Get last error string of transport (if specific) or ZMQ 
        :return: 
        """
        if self.last_error < -64000:
            if self.last_error == TRANSPORT_ERR_BAD_SIZE:
                return 'Transport data size has less than TransportHeader size'
            elif self.last_error == TRANSPORT_ERR_BAD_HEADER:
                return 'Transport invalid header signature'
            elif self.last_error == TRANSPORT_ERR_BAD_PARTSCOUNT:
                return 'Transport unexpected multipart count'
            elif self.last_error == TRANSPORT_ERR_SOCKET_CLOSED:
                return 'Socket already closed'
            elif self.last_error == TRANSPORT_ERR_NULL_DATA:
                return "Data is NULL"
            elif self.last_error == TRANSPORT_ERR_NULL_DEALERID:
                return "Dealer ID is mandatory for ZMQ_ROUTER.send()"
            else:
                return 'Generic transport error'

        return zmq_strerror(errnum)

    cdef int _send_set_error(self, int err_code, void* free_data) nogil:
        self.last_error = err_code
        self.msg_errors += 1

        if free_data != NULL:
            if free_data == self.last_data_received_ptr:
                # Trying to change data inplace, this is DEFINITELY dangerous
                cyassert(free_data != self.last_data_received_ptr)  #f'Trying to send previously received data with no_copy=False, this is DEFINITELY dangerous'

            # Free the data to avoid memory leaks
            free(free_data)

        return err_code

    cdef int send(self, char *topic_or_dealer, void *data, size_t size, bint no_copy)  nogil:
        """
        Send data via transport
        
        :param topic_or_dealer: can be NULL (if skip), you also must set dealer sender ID, which you ge in received data header,
                                TransportHeader.sender_id to route messages accordingly to the dealer.
                                For PUB/SUB transports this field used as topic
        :param data: data structure, must include space for TransportHeader (at the beginning) + some useful data for protocol
        :param size: data size
        :param no_copy: 
        :return: number of bytes sent to the socket
        """
        self.last_error = TRANSPORT_ERR_OK
        cdef int rc = 0

        if self.socket == NULL:
            return self._send_set_error(TRANSPORT_ERR_SOCKET_CLOSED, data if no_copy else NULL)

        if data == NULL:
            return self._send_set_error(TRANSPORT_ERR_NULL_DATA, data if no_copy else NULL)

        if size <= 0 or size < sizeof(TransportHeader):
            return self._send_set_error(TRANSPORT_ERR_BAD_SIZE, data if no_copy else NULL)

        # Sending dealer ID for ZMQ_ROUTER is mandatory!
        if self.socket_type == ZMQ_ROUTER and topic_or_dealer == NULL:
            return self._send_set_error(TRANSPORT_ERR_NULL_DEALERID, data if no_copy else NULL)

        if topic_or_dealer != NULL:
            rc = zmq_send(self.socket, topic_or_dealer, strlen(topic_or_dealer), ZMQ_SNDMORE)

            if rc == -1:
                return self._send_set_error(TRANSPORT_ERR_ZMQ, data if no_copy else NULL)


        cdef zmq_msg_t msg
        cdef TransportHeader* hdr = <TransportHeader*>data
        hdr.magic_number = TRANSPORT_HDR_MGC
        strlcpy(hdr.sender_id, self.transport_id, TRANSPORT_SENDER_SIZE + 1)

        if no_copy:
            if data == self.last_data_received_ptr:
                # Trying to change data inplace, this is DEFINITELY dangerous
                cyassert(data != self.last_data_received_ptr)  #f'Trying to send previously received data with no_copy=False, this is DEFINITELY dangerous'
            rc = zmq_msg_init_data (&msg, data, size, <zmq_free_fn*>_zmq_free_data_callback, NULL)
        else:
            rc = zmq_msg_init_size (&msg, size)
            # Using zmq_msg_data(&msg) as in example: http://api.zeromq.org/master:zmq-msg-send
            memcpy(zmq_msg_data(&msg), data, size)

        # Data initialization failure (out of mem?)
        cyassert(rc == 0)

        rc = zmq_msg_send(&msg, self.socket, 0)
        # Sending failure
        if rc == -1:
            # Don't free the data assuming that it's a job for ZMQ
            return self._send_set_error(TRANSPORT_ERR_ZMQ, NULL)

        cyassert(<size_t>rc == size)

        self.msg_sent += 1

        return rc

    cdef void receive_finalize(self, void *data)  nogil:
        """
        Clean up received data message
        
        You must finalize data when you done, i.e.:
            > data = transport.receive(&data_size)
            > ... processing here ...
            > transport.receive_finalize(data)
        
        :param data: must be the same pointer as you get from transport.receive() 
        :return: 
        """
        # Check if not already finalized
        cyassert(self.last_msg_received_ptr != NULL) #, f'You are trying to finalize not received data or multiple receive_finalize() calls'

        # Check if the finalized data pointer address equal
        cyassert (self.last_data_received_ptr == data) #, f'Make sure that you use previously received data pointer in this function'

        cdef int rc = zmq_msg_close(&self.last_msg)
        # The zmq_msg_close() function shall return zero if successful.
        # Otherwise it shall return -1 and set errno to one of the values defined below.
        cyassert(rc == 0)

        # Clean up
        self.last_msg_received_ptr = NULL
        self.last_data_received_ptr = NULL

    cdef void * _receive_set_error(self, int errcode, size_t *size, bint close_msg) nogil:
        self.last_error = errcode
        size[0] = 0
        self.msg_errors += 1
        self.last_data_received_ptr = NULL
        self.last_msg_received_ptr = NULL
        if close_msg:
            rc = zmq_msg_close(&self.last_msg)
            cyassert(rc == 0)

        return NULL

    cdef void * receive(self, size_t *size) nogil:
        """
        Receive a dynamically allocated message
        
        You must finalize data when you done, i.e.:
            > data = transport.receive(&data_size)
            > ... processing here ...
            > transport.receive_finalize(data)
        
        Next transport.receive without finalized will raise assert
        
        :param size: pointer to the message size
        :return:  pointer to data, or NULL on error!
        """
        self.last_error = TRANSPORT_ERR_OK

        # Make sure that previous call called receive_finalize()
        #    or protocol calls req_finalize() when it's done!!!
        cyassert(self.last_msg_received_ptr == NULL) #, 'Make sure that previous call called receive_finalize(), before next receive()'

        if self.socket == NULL:
            self.last_error = TRANSPORT_ERR_SOCKET_CLOSED
            return self._receive_set_error(TRANSPORT_ERR_SOCKET_CLOSED, size, 0)

        cdef int rc = 0
        cdef int msg_part = 0
        cdef void * data
        cdef TransportHeader* hdr

        while True:
            rc = zmq_msg_init(&self.last_msg)
            cyassert(rc == 0)

            rc = zmq_msg_recv(&self.last_msg, self.socket, 0)
            if rc == -1:
                # ZMQ Receive error
                return self._receive_set_error(TRANSPORT_ERR_ZMQ, size, 0)

            size[0] = zmq_msg_size(&self.last_msg)

            msg_part += 1

            if not zmq_msg_more(&self.last_msg):
                #printf('%s Last Frame: bytes %d\n', self.transport_id, size[0])
                break # Last frame arrived
            else:
                #printf('%s Multipart message: %d bytes\n', self.transport_id, size[0])
                rc = zmq_msg_close(&self.last_msg)
                cyassert(rc == 0)

        if self.socket_type == ZMQ_DEALER and msg_part != 1:
            return self._receive_set_error(TRANSPORT_ERR_BAD_PARTSCOUNT, size, 1)
        elif self.socket_type == ZMQ_ROUTER and msg_part != 2:
            return self._receive_set_error(TRANSPORT_ERR_BAD_PARTSCOUNT, size, 1)
        else:
            data = zmq_msg_data(&self.last_msg)
            if size[0] < sizeof(TransportHeader):
                return self._receive_set_error(TRANSPORT_ERR_BAD_SIZE, size, 1)
            else:
                hdr = <TransportHeader*>data
                if hdr.magic_number != TRANSPORT_HDR_MGC:
                    return self._receive_set_error(TRANSPORT_ERR_BAD_HEADER, size, 1)

            self.last_msg_received_ptr = &self.last_msg
            self.last_data_received_ptr = data
            self.msg_received += 1
            return self.last_data_received_ptr

    cdef void close(self) nogil:
        cdef int timeout = 0  # 2 seconds
        self.last_error = TRANSPORT_ERR_SOCKET_CLOSED

        if self.socket != NULL:
            zmq_setsockopt(self.socket, ZMQ_LINGER, &timeout, sizeof(int))
            zmq_close(self.socket)
            self.socket = NULL

        # Not finalized receive but closing socket!
        cyassert(self.last_msg_received_ptr == NULL)

    def __dealloc__(self):
        cdef int timeout = 0  # 2 seconds

        if self.socket != NULL:
            zmq_setsockopt(self.socket, ZMQ_LINGER, &timeout, sizeof(int))
            zmq_close(self.socket)

