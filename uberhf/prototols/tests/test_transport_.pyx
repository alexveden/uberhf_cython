import time
import unittest
import zmq
# cdef-classes require cimport and .pxd file!
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport strlcpy
from libc.stdint cimport uint64_t
from libc.string cimport memcmp, strlen, strcmp, memcpy
from libc.stdlib cimport malloc, free
from uberhf.prototols.messages cimport *
from uberhf.includes.asserts cimport cybreakpoint

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
            transport = None
            try:
                transport = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLIEN', router_id=b'SRVER')
                assert TRANSPORT_SENDER_SIZE == 6, 'Max sender size'
                assert transport.socket != NULL, 'Socket must be initialized'
                assert transport.last_error == 0, 'last error 0'
                assert transport.context != NULL, f'context must be stored'
                assert memcmp(transport.transport_id, b'CLIEN', TRANSPORT_SENDER_SIZE) == 0, 'transport.transport_id no match'
                assert memcmp(transport.transport_id, b'CLIEN\0', TRANSPORT_SENDER_SIZE) == 0, 'transport.transport_id no match'
                assert strcmp(transport.transport_id, b'CLIEN') == 0, 'transport.transport_id no match'
                assert transport.socket_type == ZMQ_DEALER

                assert memcmp(transport.router_id, b'SRVER', TRANSPORT_SENDER_SIZE) == 0, 'transport.router_id no match'
                assert memcmp(transport.router_id, b'SRVER\0', TRANSPORT_SENDER_SIZE) == 0, 'transport.router_id no match'
                assert strcmp(transport.router_id, b'SRVER') == 0, 'transport.router_id no match'

                socket = zmq.Socket.shadow(<uint64_t>transport.socket)
                socket_routing_id = socket.get(zmq.ROUTING_ID)
                assert len(socket_routing_id) == 5, f'ZMQ_ROUTING_ID length'
                assert socket_routing_id == b'CLIEN'
                assert transport.last_error == 0
            except:
                raise
            finally:
                if transport is not None:
                    transport.close()
                    assert transport.last_error == TRANSPORT_ERR_SOCKET_CLOSED

    def test_init_router(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        with zmq.Context() as ctx:
            transport = None
            try:
                transport = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'C')
                assert TRANSPORT_SENDER_SIZE == 6, 'Max sender size'
                assert transport.socket != NULL, 'Socket must be initialized'
                assert transport.last_error == 0, 'last error 0'
                assert transport.context != NULL, f'context must be stored'
                assert memcmp(transport.transport_id, b'C', 1) == 0, 'transport.transport_id no match'
                assert transport.socket_type == ZMQ_ROUTER

                socket = zmq.Socket.shadow(<uint64_t>transport.socket)
                socket_routing_id = socket.get(zmq.ROUTING_ID)
                assert socket_routing_id == b'C', f'ZMQ_ROUTING_ID length'
            except:
                raise
            finally:
                if transport is not None:
                    transport.close()
                    assert transport.last_error == TRANSPORT_ERR_SOCKET_CLOSED
                    assert transport.socket == NULL

    def test_simple_client_request(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1) # Sleep to make connection established, because transport_c is non-blocking!
                msg.data = 777
                self.assertGreater(transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False), 0, transport_c.get_last_error_str(transport_c.get_last_error()))
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
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_simple_server_response(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!
                msg.data = 777
                self.assertGreater(transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False), 0)
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
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_null_data_handling(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                msg.data = 777
                result = transport_c.send(NULL, NULL, sizeof(TestGenericMessage), no_copy=False)
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_NULL_DATA
                assert transport_c.get_last_error() == TRANSPORT_ERR_NULL_DATA
                assert transport_c.get_last_error_str(result) == b"Data is NULL"
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_size_handling(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                msg.data = 777
                result = transport_c.send(NULL, &msg, 0, no_copy=False)
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_BAD_SIZE
                assert transport_c.get_last_error() == TRANSPORT_ERR_BAD_SIZE
                assert transport_c.get_last_error_str(result) == b'Transport data size has less than TransportHeader size'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_incoming__bad_header(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg


        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                msg.header.magic_number = 2344
                pmsg = &msg
                memcpy(buffer, &msg, sizeof(TestGenericMessage))

                socket.send_multipart([b'SRV', buffer[:sizeof(TestGenericMessage)]])
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_HEADER
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_HEADER) == b'Transport invalid header signature'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_incoming__bad_size(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:

            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                socket.send_multipart([b'SRV', b'test'])
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_SIZE
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_SIZE) == b'Transport data size has less than TransportHeader size'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not None:
                    transport_c.close()

    def test_bad_data_incoming__bad_size__multipart(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!
                socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

                socket.send_multipart([b'SRV', b'test', b'not good'])
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_BAD_PARTSCOUNT
                assert transport_s.get_last_error_str(TRANSPORT_ERR_BAD_PARTSCOUNT) == b'Transport unexpected multipart count'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_incoming__bad_size__multipart_dealer(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                socket = zmq.Socket.shadow(<uint64_t> transport_s.socket)

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
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_incoming__receive_socket_closed(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)

                socket.send_multipart([b'SRV', b'test', b'not good'])
                transport_s.close()
                pmsg = <TestGenericMessage *> transport_s.receive(&buffer_size)
                assert buffer_size == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert pmsg == NULL
                assert transport_s.get_last_error() == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_s.get_last_error_str(TRANSPORT_ERR_SOCKET_CLOSED) == b'Socket already closed'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    def test_bad_data_incoming__send_socket_closed(self):
        cdef char buffer[255]
        cdef size_t buffer_size = 2

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                msg.data = 777
                transport_c.close()
                result = transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)

                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1
                assert result == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_c.get_last_error() == TRANSPORT_ERR_SOCKET_CLOSED
                assert transport_c.get_last_error_str(TRANSPORT_ERR_SOCKET_CLOSED) == b'Socket already closed'
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

    @pytest.mark.skipif((os.environ.get('COVERAGE_RUN') is not None), reason='ZMQ and GIL lock conflict, when trying to free no_copy buffer')
    def test_simple_server_response__no_copy_valid(self):
        """
        THIS TEST MAKES COVERAGE CORE DUMP (because _zmq_free_data_callback function called outside of GIL)!
        """
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage * msg1 =<TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * msg2 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * msg3 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!
                assert zmq_free_count == 0
                msg1.data = 777
                assert transport_c.send(NULL, msg1, sizeof(TestGenericMessage), no_copy=True) > 0
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)
                if pmsg == NULL:
                    assert transport_s.get_last_error() == 0, transport_s.get_last_error_str(transport_s.get_last_error())
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

                transport_c.close()

                # When ZMQ failure, the zmq-free callback also must be called!
                assert transport_s.send(pmsg.header.sender_id, msg3, sizeof(TestGenericMessage), no_copy=True) == -64000
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

        assert zmq_free_count == 3, int(zmq_free_count)
    #
    def test_simple_server_response__no_copy_no_free(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage * msg1 =<TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * msg2 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!
                assert zmq_free_count == 0
                msg1.header.magic_number = TRANSPORT_HDR_MGC
                strlcpy(msg1.header.sender_id, transport_c.transport_id, TRANSPORT_SENDER_SIZE)
                msg1.data = 777
                assert transport_c.send(NULL, msg1, sizeof(TestGenericMessage), no_copy=-1) > 0
                assert transport_c.msg_sent == 1
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 0

                #
                # Server received
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)
                if pmsg == NULL:
                    assert transport_s.get_last_error() == 0, transport_s.get_last_error_str(transport_s.get_last_error())
                assert pmsg != NULL
                assert transport_s.msg_errors == 0
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 1
                assert pmsg.data == 777


                # server reply
                msg2.header.magic_number = TRANSPORT_HDR_MGC
                strlcpy(msg2.header.sender_id, transport_s.transport_id, TRANSPORT_SENDER_SIZE)
                #cybreakpoint(1)
                assert strcmp(pmsg.header.sender_id, transport_c.transport_id) == 0
                msg2.data = 888
                res = transport_s.send(pmsg.header.sender_id, msg2, sizeof(TestGenericMessage), no_copy=-1)
                assert res > 0, int(res)
                transport_s.receive_finalize(pmsg)
                if res <= 0:
                    assert transport_s.get_last_error() == 0, transport_s.get_last_error_str(transport_s.get_last_error())


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
                assert pmsg.data == 888
                transport_c.receive_finalize(pmsg)
            except:
                raise
            finally:

                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()

        assert zmq_free_count == 0, int(zmq_free_count)
        free(msg1)
        free(msg2)

    def test_simple_server_response__no_copy_invalid(self):
        """
        This test for manual handling of weird
        :return:
        """
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage * msg1
        cdef TestGenericMessage * msg2
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_c = None
            try:
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!
                assert zmq_free_count == 0
                msg1 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
                msg1.data = 777
                assert transport_c.send(NULL, msg1, sizeof(TransportHeader)-1, no_copy=True) < 0
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 1

                msg1 = <TestGenericMessage *> malloc(sizeof(TestGenericMessage))
                assert transport_c.send(NULL, msg1, 0, no_copy=True) < 0
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 2

            except:
                raise
            finally:

                if transport_c is not None:
                    transport_c.close()

        assert zmq_free_count == 2, zmq_free_count


    def test_simple_client_request_no_server(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef void * data
        cdef size_t data_size

        with zmq.Context() as ctx:
            transport_c = None
            transport_s = None
            try:
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                msg.data = 777
                result = transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                assert result == TRANSPORT_ERR_ZMQ
                assert transport_c.msg_errors == 1
                assert transport_c.last_error == TRANSPORT_ERR_ZMQ

                data = transport_c.receive(&data_size)
                assert transport_c.msg_sent == 0
                assert transport_c.msg_received == 0
                assert transport_c.msg_errors == 2
                assert data == NULL
                assert data_size == 0
                assert transport_c.last_error == TRANSPORT_ERR_ZMQ
                # Error codes returned from ZMQ space
                assert transport_c.get_last_error() == zmq_errno()
                assert transport_c.get_last_error_str(transport_c.get_last_error()) == zmq_strerror(zmq_errno()), zmq_strerror(zmq_errno())
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not None:
                    transport_c.close()

    def test_simple_client_request_no_server_poller(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef void * data
        cdef size_t data_size

        cdef zmq_pollitem_t poll_items[1]

        with zmq.Context() as ctx:
            transport_c = None
            transport_s = None

            try:
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV')
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                transport_s = None

                poll_items[0] = [transport_c.socket, 0, ZMQ_POLLIN, 0]

                msg.data = 777
                result = transport_c.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                assert result == TRANSPORT_ERR_ZMQ

                while True:
                    # Poll isn't affected by recv timeouts!
                    rc = zmq_poll(poll_items, 1, 400)
                    assert rc == 0
                    # We must deal with timeouts on protocol levels!
                    break
            except:
                raise
            finally:

                if transport_c is not None:
                    transport_c.close()

    def test_simple_pub_sub(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_c = None
            transport_s = None
            transport_c2 = None


            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_PUB, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI')
                transport_c2 = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI')

                # socket_s = zmq.Socket.shadow(<uint64_t> transport_s.socket)
                # self.assertEqual(b'bbb', socket_s.get(zmq.BINDTODEVICE))

                # Do some sleep to make sure sub process went well
                time.sleep(0.5)
                #cybreakpoint(1)

                msg.data = 777
                transport_s.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                # Change of this value must not affect the received value
                msg.data = 888

                pmsg = <TestGenericMessage*>transport_c.receive(&buffer_size)
                assert pmsg != NULL
                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 777  # <<<---- this still must be 777, not 888
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(pmsg.header.sender_id, b'SRV') == 0

                assert transport_c.last_error == 0
                assert transport_c.last_msg_received_ptr != NULL
                assert transport_c.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_c.receive_finalize(pmsg)

                pmsg = <TestGenericMessage*>transport_c2.receive(&buffer_size)
                assert pmsg != NULL
                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 777  # <<<---- this still must be 777, not 888
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(pmsg.header.sender_id, b'SRV') == 0

                assert transport_c2.last_error == 0
                assert transport_c2.last_msg_received_ptr != NULL
                assert transport_c2.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_c2.receive_finalize(pmsg)
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()
                if transport_c2 is not None:
                    transport_c2.close()


    def test_simple_pub_sub_reverse_bind_pyzmq(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            socket_s = None
            socket_c = None

            try:
                socket_s = ctx.socket(zmq.SUB)
                socket_c = ctx.socket(zmq.PUB)

                socket_s.subscribe('')
                socket_s.set(zmq.RCVTIMEO, 500)

                socket_s.bind("tcp://*:9002")
                socket_c.connect("tcp://127.0.0.1:9002")
                time.sleep(0.1)

                # https://github.com/zeromq/pyzmq/issues/1650#issuecomment-1016261063
                # I think there is a bug in libzmq when PUB connects and SUB binds, where the subscription does not propagate until a
                # poll or recv is called for the first time. This is not a pyzmq issue, but a libzmq one.
                # You can work around it by attempting a sub.poll(timeout=0) before you get started.
                #
                socket_s.poll(0)
                time.sleep(0.1)

                socket_c.send_multipart([b'ss', b'test'])

                data = socket_s.recv_multipart()
                assert [b'ss', b'test'] == data, data
            except:
                raise
            finally:
                if socket_s is not None:
                    socket_s.close()
                if socket_c is not  None:
                    socket_c.close()

    def test_simple_pub_sub_reverse_bind(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_c = None
            transport_s = None
            transport_c2 = None

            try:

                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_SUB, b'SRV', swap_bindconnect=True, socket_timeout=500)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_PUB, b'CLI1', swap_bindconnect=True, socket_timeout=500)
                transport_c2 = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_PUB, b'CLI2', swap_bindconnect=True, socket_timeout=500)
                time.sleep(0.15)

                # libzmq when PUB connects and SUB binds, where the subscription does not propagate until a
                #       poll or recv is called for the first time (should be fine on production when poller is used!)
                assert transport_s.receive(&buffer_size) == NULL
                time.sleep(0.05)

                msg.data = 777
                self.assertGreater(transport_c.send(b'', &msg, sizeof(TestGenericMessage), no_copy=False), 0)
                assert transport_c.msg_sent == 1
                time.sleep(0.05)

                #self.assertGreater(transport_c.send(b'', &msg, sizeof(TestGenericMessage), no_copy=False), 0)

                #time.sleep(0.05)
                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)
                assert pmsg != NULL
                assert transport_s.last_error == 0, transport_s.get_last_error_str(transport_s.get_last_error())
                assert transport_s.get_last_error() == TRANSPORT_ERR_OK
                assert transport_s.get_last_error_str(transport_s.get_last_error()) == b'No error, last operation succeeded'

                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 777
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(pmsg.header.sender_id, b'CLI1') == 0

                assert transport_s.last_error == 0
                assert transport_s.last_msg_received_ptr != NULL
                assert transport_s.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_s.receive_finalize(pmsg)

                msg.data = 888
                transport_c2.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)

                pmsg = <TestGenericMessage*>transport_s.receive(&buffer_size)
                assert pmsg != NULL
                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 888
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(pmsg.header.sender_id, b'CLI2') == 0

                assert transport_s.last_error == 0
                assert transport_s.last_msg_received_ptr != NULL
                assert transport_s.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_s.receive_finalize(pmsg)

            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not  None:
                    transport_c.close()
                if transport_c2 is not None:
                    transport_c2.close()


    def test_simple_pub_sub_with_topic(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:
            transport_c = None
            transport_s = None
            transport_c2 = None

            try:

                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_PUB, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI', sub_topic=b'important')
                transport_c2 = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI', sub_topic=b'test_topic_excluded')

                # Do some sleep to make sure sub process went well
                time.sleep(0.5)

                msg.data = 777

                # transport_c - will receive because sub_topic - compares prefix!
                transport_s.send(b'important_stuff', &msg, sizeof(TestGenericMessage), no_copy=False)
                # Change of this value must not affect the received value
                msg.data = 888

                pmsg = <TestGenericMessage*>transport_c.receive(&buffer_size)
                assert pmsg != NULL
                assert buffer_size == sizeof(TestGenericMessage)
                assert pmsg.data == 777  # <<<---- this still must be 777, not 888
                assert pmsg.header.magic_number == TRANSPORT_HDR_MGC
                assert strcmp(pmsg.header.sender_id, b'SRV') == 0

                assert transport_c.last_error == 0
                assert transport_c.last_msg_received_ptr != NULL
                assert transport_c.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_c.receive_finalize(pmsg)

                # Nothing to receive
                pmsg = <TestGenericMessage*>transport_c2.receive(&buffer_size)
                assert pmsg == NULL
                assert transport_c2.last_error == TRANSPORT_ERR_ZMQ
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not None:
                    transport_c.close()
                if transport_c2 is not None:
                    transport_c2.close()


    def test_simple_pub_sub_with_multi_topic(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:

            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_PUB, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI', sub_topic=b'important')
                transport_c2 = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_SUB, b'CLI', sub_topic=[b'test_topic_excluded', b'multi'])

                # Do some sleep to make sure sub process went well
                time.sleep(0.5)

                msg.data = 777

                # transport_c - will receive because sub_topic - compares prefix!
                transport_s.send(b'important_stuff', &msg, sizeof(TestGenericMessage), no_copy=False)

                msg.data = 888
                transport_s.send(b'multi_with_another_prefix', &msg, sizeof(TestGenericMessage), no_copy=False)

                pmsg = <TestGenericMessage*>transport_c.receive(&buffer_size)
                assert pmsg != NULL
                assert pmsg.data == 777
                assert strcmp(pmsg.header.sender_id, b'SRV') == 0

                assert transport_c.last_error == 0
                assert transport_c.last_msg_received_ptr != NULL
                assert transport_c.last_data_received_ptr == pmsg
                #
                # # Finalizing the message
                transport_c.receive_finalize(pmsg)

                # Nothing to receive
                pmsg = <TestGenericMessage*>transport_c2.receive(&buffer_size)
                assert pmsg != NULL
                assert pmsg.data == 888
                transport_c2.receive_finalize(pmsg)
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
                if transport_c is not None:
                    transport_c.close()
                if transport_c2 is not None:
                    transport_c2.close()

    def test_server_send_without_clients(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:

            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')

                msg.data = 777
                result = transport_s.send(b'CLI', &msg, sizeof(TestGenericMessage), no_copy=False)
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert result == TRANSPORT_ERR_ZMQ

                assert transport_s.get_last_error() == 113, transport_s.get_last_error()
                assert transport_s.get_last_error_str(transport_s.get_last_error()) ==  b'Host unreachable'
            except:
                raise
            finally:
                transport_s.close()

    def test_server_send_topic_is_mandatory(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:

            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                #transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

                msg.data = 777
                result = transport_s.send(NULL, &msg, sizeof(TestGenericMessage), no_copy=False)
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert result == TRANSPORT_ERR_NULL_DEALERID
                assert transport_s.get_last_error() == TRANSPORT_ERR_NULL_DEALERID
                assert transport_s.get_last_error_str(TRANSPORT_ERR_NULL_DEALERID) == b"Dealer ID is mandatory for ZMQ_ROUTER.send()"
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()


    def test_server_send_topic_empty_is_mandatory(self):
        cdef char buffer[255]
        cdef size_t buffer_size

        cdef TestGenericMessage msg
        cdef TestGenericMessage * pmsg

        with zmq.Context() as ctx:

            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                msg.data = 777
                result = transport_s.send(b'', &msg, sizeof(TestGenericMessage), no_copy=False)
                assert transport_s.msg_sent == 0
                assert transport_s.msg_received == 0
                assert transport_s.msg_errors == 1
                assert result == TRANSPORT_ERR_NULL_DEALERID
                assert transport_s.get_last_error() == TRANSPORT_ERR_NULL_DEALERID
                assert transport_s.get_last_error_str(TRANSPORT_ERR_NULL_DEALERID) == b"Dealer ID is mandatory for ZMQ_ROUTER.send()"
            except:
                raise
            finally:
                if transport_s is not None:
                    transport_s.close()
