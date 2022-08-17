import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from libc.stdint cimport uint64_t
from libc.string cimport memcmp, strlen, strcmp
from uberhf.prototols.libzmq cimport *

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
