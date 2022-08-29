from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, strcmp
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *
from uberhf.includes.utils cimport strlcpy
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.includes.uhfprotocols cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from uberhf.prototols.messages cimport TransportHeader

# It's only for testing purposes to make sure if ZMQ correctly frees memory
from .transport cimport zmq_free_count

cdef void _zmq_free_data_callback(void *data, void *hint):
    # TODO: this function is called with GIL! If nogil tests will fail with ZMQ error, however this may be a bottleneck on production!

    # This is called by ZeroMQ internally, when sending with no_copy=True
    #
    #   The `void *data` must be malloc'ed previously somewhere in the user-space code!
    #
    # IMPORTANT: if you get bad address assertion/error in this place this means that you possibly
    #            tried no_copy=True send on local variable address
    #
    global zmq_free_count
    zmq_free_count += 1
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
    def __cinit__(self,
                  zmq_context_ptr,
                  socket_endpoint,
                  socket_type,
                  transport_id,
                  router_id=None,
                  socket_timeout=100,
                  sub_topic=None,
                  always_send_copy=False,
                  swap_bindconnect=False):
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
        :param router_id: servers' transport ID, for  ZMQ_DEALER socket type connection
        :param socket_timeout: socket sync receive/send timeout (this typically does not affect zmq_poll!)
        :param sub_topic:
        :param always_send_copy: (for testing purposes), make always send a copy, because in unit test environment or coverage it could be a
                                  collision between Pytho No-GIL mode and ZMQ Free
        :return:
        """
        global zmq_free_count
        zmq_free_count = 0

        self.context = <void*>(<uint64_t>zmq_context_ptr)
        assert self.context != NULL, f'Context got NULL pointer!'

        self.last_msg_received_ptr = NULL
        self.msg_errors = 0
        self.msg_sent = 0
        self.msg_received = 0
        self.last_error = TRANSPORT_ERR_OK

        assert len(transport_id) > 0, f'transport_id must be a byte string of [1; {TRANSPORT_SENDER_SIZE-1}] size'
        assert len(transport_id) <= TRANSPORT_SENDER_SIZE-1, f'transport_id must be short, <= {TRANSPORT_SENDER_SIZE-1}'
        strlcpy(self.transport_id, <char*>transport_id, TRANSPORT_SENDER_SIZE)

        self.socket_type = socket_type
        if socket_type == ZMQ_DEALER:
            #
            # For this case ZMQ_DEALER = SHADOW ZMQ_ROUTER
            #  Which is necessary for non-blocking mode when send error or overflow
            if router_id is None:
                raise ValueError(f'You must set valid `router_id` (i.e. transport_id of the server) to make this Transport connection work properly')

            assert len(router_id) > 0, f'router_id must be a byte string of [1; {TRANSPORT_SENDER_SIZE - 1}] size'
            assert len(router_id) <= TRANSPORT_SENDER_SIZE - 1, f'router_id must be short, <= {TRANSPORT_SENDER_SIZE - 1}'
            strlcpy(self.router_id, <char *>router_id, TRANSPORT_SENDER_SIZE)
            # Our dealer is 1-way router, but without blocking when high-water-mark overflow!
            self.socket = zmq_socket(self.context, ZMQ_ROUTER)
        else:
            self.router_id[0] = b'\0'  # Just empty string (unused)
            self.socket = zmq_socket(self.context, self.socket_type)

        self.always_send_copy = always_send_copy

        if self.socket == NULL:
            raise ZMQError()

        cdef int result = 0
        if self.socket_type == ZMQ_ROUTER:
            # Set routing ID for a ZMQ_ROUTER to make SHADOW ZMQ_ROUTER (client side) work
            zmq_setsockopt(self.socket, ZMQ_ROUTING_ID, self.transport_id, len(transport_id))
            self._socket_set_option(ZMQ_ROUTER_MANDATORY, 1)
            self._socket_set_option(ZMQ_SNDTIMEO, <int>socket_timeout)
            self._socket_set_option(ZMQ_IMMEDIATE, 1)
            #self._socket_set_option(ZMQ_SNDHWM, 10)
            self._socket_set_option(ZMQ_LINGER, 0)
            # Binding as server
            result = zmq_bind(self.socket, <char *> socket_endpoint)
        elif self.socket_type == ZMQ_DEALER:
            self._socket_set_option(ZMQ_ROUTER_MANDATORY, 1)
            zmq_setsockopt(self.socket, ZMQ_ROUTING_ID, self.transport_id, len(self.transport_id))

            self._socket_set_option(ZMQ_IMMEDIATE, 1)
            self._socket_set_option(ZMQ_RCVTIMEO, socket_timeout)
            self._socket_set_option(ZMQ_SNDTIMEO, socket_timeout)
            self._socket_set_option(ZMQ_SNDHWM, 0)
            self._socket_set_option(ZMQ_LINGER, 0)
            # Connecting as client
            result = zmq_connect(self.socket, <char *> socket_endpoint)
        elif self.socket_type == ZMQ_PUB:
            if sub_topic is not None:
                raise ValueError(f'sub_topic applicable only for clients, ZMQ_SUB sockets')

            #self._socket_set_option(ZMQ_LINGER, 0)
            self._socket_set_option(ZMQ_RCVTIMEO, socket_timeout)
            self._socket_set_option(ZMQ_SNDTIMEO, socket_timeout)

            # Binding as server
            if swap_bindconnect:
                # Swapped behaviour
                result = zmq_connect(self.socket, <char *> socket_endpoint)
            else:
                # Regular behaviour
                result = zmq_bind(self.socket, <char *> socket_endpoint)

        elif self.socket_type == ZMQ_SUB:
            # Topics
            if sub_topic is None:
                zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, b'', 0)
            elif isinstance(sub_topic, list):
                for s in sub_topic:
                    zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, <char*>s, len(s))
            else:
                zmq_setsockopt(self.socket, ZMQ_SUBSCRIBE, <char*>sub_topic, len(sub_topic))

            # General socket settings
            self._socket_set_option(ZMQ_RCVTIMEO, socket_timeout)
            self._socket_set_option(ZMQ_SNDTIMEO, socket_timeout)
            #self._socket_set_option(ZMQ_LINGER, 0)

            if swap_bindconnect:
                # Swapped behaviour
                result = zmq_bind(self.socket, <char *> socket_endpoint)
            else:
                # Regular behaviour
                result = zmq_connect(self.socket, <char *> socket_endpoint)

        else:
            raise NotImplementedError(f'Unsupported socket type: {self.socket_type}')

        if result != 0:
            raise ZMQError()

    cdef _socket_set_option(self, int zmq_opt, int value):
        cdef int v = value
        zmq_setsockopt(self.socket, zmq_opt, &v, sizeof(int))

    cdef int get_last_error(self) nogil:
        """
        Get last error code of transport (if specific) or ZMQ 
        :return: 
        """
        if self.last_error == TRANSPORT_ERR_OK:
            return TRANSPORT_ERR_OK

        if self.last_error < -64000:
            return self.last_error

        return zmq_errno()

    cdef const char* get_last_error_str(self, int errnum) nogil:
        """
        Get last error string of transport (if specific) or ZMQ 
        :return: 
        """
        if self.last_error == TRANSPORT_ERR_OK:
            return 'No error, last operation succeeded'

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

    cdef int _send_set_error(self, int err_code, void* free_data, int no_copy) nogil:
        global zmq_free_count
        self.last_error = err_code
        self.msg_errors += 1

        if free_data != NULL and no_copy == 1:
            # Free data only when no_copy == 1. Those are skipped: no_copy=-1, no_copy=0
            if free_data == self.last_data_received_ptr:
                # Trying to change data inplace, this is DEFINITELY dangerous
                cyassert(free_data != self.last_data_received_ptr)  #f'Trying to send previously received data with no_copy=False, this is DEFINITELY dangerous'

            # Free the data to avoid memory leaks
            free(free_data)
            zmq_free_count += 1

        return err_code

    cdef int send(self, char *topic_or_dealer, void *data, size_t size, int no_copy)  nogil:
        """
        Send data via transport
        
        :param topic_or_dealer: can be NULL (if skip), you also must set dealer sender ID, which you ge in received data header,
                                TransportHeader.sender_id to route messages accordingly to the dealer.
                                For PUB/SUB transports this field used as topic
        :param data: data structure, must include space for TransportHeader (at the beginning) + some useful data for protocol
        :param size: data size
        :param no_copy: 
            0 - data is fully copied, 
            1 - data not copied, headers are also filled, data will be freed later, 
           -1 - data not copied and NOT freed (useful for **long standing buffers**!), manual headers and sender_id fill required (sent as is!) 
        :return: number of bytes sent to the socket
        """
        self.last_error = TRANSPORT_ERR_OK
        cdef int rc = 0

        if self.socket == NULL:
            return self._send_set_error(TRANSPORT_ERR_SOCKET_CLOSED, data, no_copy)

        if data == NULL:
            return self._send_set_error(TRANSPORT_ERR_NULL_DATA, data,  no_copy)

        if size < sizeof(TransportHeader):
            return self._send_set_error(TRANSPORT_ERR_BAD_SIZE, data, no_copy)

        # Sending dealer ID for ZMQ_ROUTER is mandatory!
        if self.socket_type == ZMQ_ROUTER and (topic_or_dealer == NULL or topic_or_dealer[0] == b'\0'):
            return self._send_set_error(TRANSPORT_ERR_NULL_DEALERID, data, no_copy)

        if topic_or_dealer != NULL and topic_or_dealer[0] != b'\0':
            rc = zmq_send(self.socket, topic_or_dealer, strlen(topic_or_dealer), ZMQ_SNDMORE)
            cybreakpoint(strcmp(self.transport_id, b'CLRB1') == 0)

            if rc == -1:
                return self._send_set_error(TRANSPORT_ERR_ZMQ, data, no_copy)
        elif self.socket_type == ZMQ_DEALER:
            rc = zmq_send(self.socket, self.router_id, strlen(self.router_id), ZMQ_SNDMORE)
            if rc == -1:
                # Don't free the data assuming that it's a job for ZMQ
                return self._send_set_error(TRANSPORT_ERR_ZMQ, NULL, no_copy)

        cdef zmq_msg_t msg
        cdef TransportHeader* hdr = <TransportHeader*>data

        if no_copy != -1:
            hdr.magic_number = TRANSPORT_HDR_MGC
            strlcpy(hdr.sender_id, self.transport_id, TRANSPORT_SENDER_SIZE)
        else:
            cyassert(hdr.magic_number == TRANSPORT_HDR_MGC)  # You must fully initialize headers in user code!

        if no_copy and not self.always_send_copy:
            # Trying to change data inplace, this is DEFINITELY dangerous
            cyassert(data != self.last_data_received_ptr)  #f'Trying to send previously received data with no_copy=False, this is DEFINITELY dangerous'

            if no_copy == 1:

                rc = zmq_msg_init_data (&msg, data, size, <zmq_free_fn*>_zmq_free_data_callback, NULL)
            else:
                cyassert(no_copy == -1)
                rc = zmq_msg_init_data(&msg, data, size, NULL, NULL)
        else:
            rc = zmq_msg_init_size (&msg, size)
            # Using zmq_msg_data(&msg) as in example: http://api.zeromq.org/master:zmq-msg-send
            memcpy(zmq_msg_data(&msg), data, size)
            if no_copy:
                # When no_copy and self.always_send_copy, we must free memory immediately
                # This is typically used for unit testing mode / coverage to avoid seg faults when NO-GIL at _zmq_free_data_callback
                free(data)

        # Data initialization failure (out of mem?)
        cyassert(rc == 0)

        rc = zmq_msg_send(&msg, self.socket, 0)
        # Sending failure
        if rc == -1:
            # Don't free the data assuming that it's a job for ZMQ
            return self._send_set_error(TRANSPORT_ERR_ZMQ, NULL, no_copy)

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
        if self.socket == NULL:
            return

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
        cyassert(self.last_data_received_ptr == NULL)

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

        if self.socket_type == ZMQ_DEALER and msg_part != 2:
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
        cdef int timeout = 0
        self.last_error = TRANSPORT_ERR_SOCKET_CLOSED

        # Not finalized receive but closing socket!
        if self.last_data_received_ptr != NULL:
           self.receive_finalize(self.last_data_received_ptr)

        if self.socket != NULL:
            zmq_setsockopt(self.socket, ZMQ_LINGER, &timeout, sizeof(int))
            zmq_close(self.socket)
            self.socket = NULL


    def __dealloc__(self):
        self.close()

