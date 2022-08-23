import time
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import unittest
import zmq
from libc.stdint cimport uint64_t
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.asserts cimport cyassert
from uberhf.prototols.protocol_base cimport ProtocolBase,  ProtocolBaseMessage, ConnectionState
from uberhf.prototols.protocol_datasource cimport ProtocolDataSourceBase
from uberhf.includes.utils cimport datetime_nsnow, sleep_ns, timedelta_ns, TIMEDELTA_SEC, timer_nsnow, TIMEDELTA_MILLI

from unittest.mock import MagicMock
URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

cdef inline bint transport_receive(Transport transport, ProtocolBaseMessage **msg) nogil:
    cdef size_t msg_size = 0
    cdef void* transport_data = transport.receive(&msg_size)

    cyassert(transport_data != NULL)
    cyassert(msg_size == sizeof(ProtocolBaseMessage))

    memcpy(msg[0], transport_data, msg_size)
    transport.receive_finalize(transport_data)
    return 1

class CyProtocolDataSourceBaseTestCase(unittest.TestCase):

    def test_protocol_connection_via_poller(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', always_send_copy=True)

                ps = ProtocolDataSourceBase(True, 11, transport_s)
                pc = ProtocolDataSourceBase(False, 22, transport_c)

                s_socket = zmq.Socket.shadow(<uint64_t> transport_s.socket)
                c_socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                poller = zmq.Poller()
                poller.register(s_socket, zmq.POLLIN)
                poller.register(c_socket, zmq.POLLIN)

                dt_prev_call = datetime_nsnow()
                for i in range(10):
                    socks = dict(poller.poll(50))
                    if s_socket in socks and socks[s_socket] == zmq.POLLIN:
                        transport_data = transport_s.receive(&msg_size)
                        assert ps.on_process_new_message(transport_data, msg_size) > 0
                        transport_s.receive_finalize(transport_data)
                    if c_socket in socks and socks[c_socket] == zmq.POLLIN:
                        transport_data = transport_c.receive(&msg_size)
                        assert pc.on_process_new_message(transport_data, msg_size) > 0
                        transport_c.receive_finalize(transport_data)

                    dt_now = datetime_nsnow()
                    if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:
                        assert ps.heartbeat(dt_now) >= 0
                        assert pc.heartbeat(dt_now) >= 0

                        dt_prev_call = dt_now

                cstate = pc.get_state(b'')
                assert cstate.status == ProtocolStatus.UHF_ACTIVE
                sstate = ps.get_state(b'CLI')
                assert sstate.status == ProtocolStatus.UHF_ACTIVE

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)
