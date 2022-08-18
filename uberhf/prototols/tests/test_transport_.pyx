import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from libc.stdint cimport uint64_t
from libc.string cimport memcmp, strlen, strcmp, memcpy
from libc.stdlib cimport malloc, free
from uberhf.prototols.libzmq cimport *
import os
import pytest

URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

ctypedef struct TestGenericMessage:
    TransportHeader header
    int data

class CyTransportTestCase(unittest.TestCase):

    def test_init_dealer(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        with zmq.Context() as ctx:
            transport = Transport(<uint64_t> ctx.underlying, URL_CONNECT , ZMQ_DEALER, b'CLIEN')
            try:
                assert TRANSPORT_SENDER_SIZE == 5, 'Max sender size'
                assert transport.socket != NULL, 'Socket must be initialized'
                assert transport.last_error == 0, 'last error 0'
                assert transport.context != NULL, f'context must be stored'
                assert transport.transport_id_len == 5, 'transport.transport_id_len == 5'
                assert memcmp(transport.transport_id, b'CLIEN', 5) == 0, 'transport.transport_id no match'
                assert transport.socket_type == ZMQ_DEALER

                socket = zmq.Socket.shadow(<uint64_t>transport.socket)
                socket_routing_id = socket.get(zmq.ROUTING_ID)
                assert len(socket_routing_id) == 5, f'ZMQ_ROUTING_ID length'
                assert socket_routing_id == b'CLIEN'
                assert transport.last_error == 0

            finally:
                transport.close()
                assert transport.last_error == TRANSPORT_ERR_SOCKET_CLOSED

    def test_init_router(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        with zmq.Context() as ctx:
            transport = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'C')
            try:
                assert TRANSPORT_SENDER_SIZE == 5, 'Max sender size'
                assert transport.socket != NULL, 'Socket must be initialized'
                assert transport.last_error == 0, 'last error 0'
                assert transport.context != NULL, f'context must be stored'
                assert transport.transport_id_len == 1, 'transport.transport_id_len == 1'
                assert memcmp(transport.transport_id, b'C', 1) == 0, 'transport.transport_id no match'
                assert transport.socket_type == ZMQ_ROUTER

                socket = zmq.Socket.shadow(<uint64_t>transport.socket)
                socket_routing_id = socket.get(zmq.ROUTING_ID)
                assert len(socket_routing_id) == 0, f'ZMQ_ROUTING_ID length'
                assert transport.last_error == 0
            finally:
                transport.close()
                assert transport.last_error == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport.socket == NULL

    def test_simple_client_request(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg.data = 777
                transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                # Change of this value must not affect the received value
                msg.data = 888

                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)
                assert pmsg != NULL
                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 777  # <<<---- this still must be 777, not 888
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(b'CLI\0', b'CLI') == 0
                assert strcmp(pmsg.header.sender_id, b'CLI') == 0

                assert transport_s.last_error == 0
                assert transport_s.last_msg_received_ptr != NULL
                assert transport_s.last_data_received_ptr == pmsg

                # Finalizing the message
                transport_s.receive_finalize(pmsg)
                assert transport_s.last_msg_received_ptr == NULL
                assert transport_s.last_data_received_ptr == NULL

            finally:
                transport_s.close()
                transport_c.close()

    def test_simple_server_response(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg.data = 777
                transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)

                assert pmsg != NULL
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 1
                pmsg.data += 1

                # server reply
                res = transport_s.send(pmsg.header.sender_id, pmsg, sizeof(TestGenericMessage), no_copy=False)
                transport_s.receive_finalize(pmsg)
                assert transport_s.msg_received == 1
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 1
                assert res == sizeof(TestGenericMessage)
                pmsg = NULL

                # Client receive
                pmsg = <TestGenericMessage *> transport_c.receive(&buffer_size)
                assert transport_c.last_error == 0, f'Errno: {transport_c.last_error}; {transport_c.get_last_error_str(transport_c.last_error)}'
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 1
                assert transport_c.msg_errors == 0

                assert pmsg != NULL
                assert pmsg.data == 778
                transport_c.receive_finalize(pmsg)

            finally:
                transport_s.close()
                transport_c.close()

    def test_null_data_handling(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg.data = 777
                result = transport_c.send(NULL, NULL, sizeof(TestGenericMessage), no_copy=False)
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_NULL_DATA
                assert transport_c.get_last_error() == TRANSPORT_ERR_NULL_DATA
                assert transport_c.get_last_error_str(result) == b"Data is NULL"

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_size_handling(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg.data = 777
                result = transport_c.send(NULL, &msg, 0, no_copy=False)
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_BAD_SIZE
                assert transport_c.get_last_error() == TRANSPORT_ERR_BAD_SIZE
                assert transport_c.get_last_error_str(result) == b'Transport data size has less than TransportHeader size'

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__bad_header(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
            msg.header.magic_number = 2344
            pmsg = &msg
            memcpy(buffer, &msg, sizeof(TestGenericMessage))

            try:
                socket.send(buffer[:sizeof(TestGenericMessage)])
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_HEADER
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_HEADER) == b'Transport invalid header signature'

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__bad_size(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

            try:
                socket.send(b'test')
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_SIZE
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_SIZE) == b'Transport data size has less than TransportHeader size'

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__bad_size__multipart(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

            try:
                socket.send_multipart([b'test', b'not good'])
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_PARTSCOUNT
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_PARTSCOUNT) == b'Transport unexpected multipart count'

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__bad_size__multipart_dealer(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_s.socket)

            try:

                msg.data = 777
                transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert pmsg != NULL
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 1
                pmsg.data += 1
                transport_s.receive_finalize(pmsg)

                # server reply
                socket.send_multipart([pmsg.header.sender_id, b'not good', b'too many parts'])


                pmsg = <TestGenericMessage *> transport_c.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert pmsg == NULL
                assert transport_c.get_last_error() == TRANSPORT_ERR_BAD_PARTSCOUNT, transport_c.get_last_error()
                assert transport_c.get_last_error_str(TRANSPORT_ERR_BAD_PARTSCOUNT) == b'Transport unexpected multipart count'

            finally:
                transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__receive_socket_closed(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

            try:
                socket.send_multipart([b'test', b'not good'])
                transport_s.close()
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_s.get_last_error_str(TRANSPORT_ERR_SOCKET_CLOSED) == b'Socket already closed'

            finally:
                #transport_s.close()
                transport_c.close()

    def test_bad_data_incoming__send_socket_closed(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

            socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

            try:
                msg.data = 777
                transport_c.close()
                result = transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)

                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_c.get_last_error() == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_c.get_last_error_str(TRANSPORT_ERR_SOCKET_CLOSED) == b'Socket already closed'

            finally:
                transport_s.close()
                #transport_c.close()

    @pytest.mark.skipif((os.environ.get('COVERAGE_RUN') is not None), reason='ZMQ and GIL lock conflict, when trying to free no_copy buffer')
    def test_simple_server_response__no_copy_valid(self):
        """
        THIS TEST MAKES COVERAGE CORE DUMP (because _zmq_free_data_callback function called outside of GIL)!
        """

        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage * msg1 =<TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * msg2 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg1.data = 777
                transport_c.send(NULL, msg1, sizeof(TestGenericMessage), no_copy=True)
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)

                assert pmsg != NULL
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 1

                # server reply
                msg2.data = pmsg.data + 1
                res = transport_s.send(pmsg.header.sender_id, msg2, sizeof(TestGenericMessage), no_copy=True)
                transport_s.receive_finalize(pmsg)
                assert transport_s.msg_received == 1
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 1
                assert res == sizeof(TestGenericMessage)
                pmsg = NULL

                # Client receive
                pmsg = <TestGenericMessage *> transport_c.receive(&buffer_size)
                assert transport_c.last_error == 0, f'Errno: {transport_c.last_error}; {transport_c.get_last_error_str(transport_c.last_error)}'
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 1
                assert transport_c.msg_errors == 0

                assert pmsg != NULL
                assert pmsg.data == 778
                transport_c.receive_finalize(pmsg)

            finally:
                transport_s.close()
                transport_c.close()


    def test_simple_server_response__no_copy_invalid(self):
        """
        This test for manual handling of weird
        :return:
        """

        # Just for manual testing, this code will always raise casserts
        return


        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage * msg1  = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * msg2
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')
            try:
                msg1.data = 777
                transport_c.send(NULL, msg1, sizeof(TestGenericMessage), no_copy=True)
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)

                assert pmsg != NULL
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 1

                # server reply
                pmsg.data += 1
                res = transport_s.send(pmsg.header.sender_id, pmsg, sizeof(TestGenericMessage), no_copy=True)
                transport_s.receive_finalize(pmsg)
                assert transport_s.msg_received == 1
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 1
                assert res == sizeof(TestGenericMessage)
                pmsg = NULL

                # Client receive
                pmsg = <TestGenericMessage *> transport_c.receive(&buffer_size)
                assert transport_c.last_error == 0, f'Errno: {transport_c.last_error}; {transport_c.get_last_error_str(transport_c.last_error)}'
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 1
                assert transport_c.msg_errors == 0

                assert pmsg != NULL
                assert pmsg.data == 778
                transport_c.receive_finalize(pmsg)

            finally:
                transport_s.close()
                transport_c.close()
