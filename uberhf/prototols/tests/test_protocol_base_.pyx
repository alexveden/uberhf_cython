import time
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import unittest
import zmq
from libc.stdint cimport uint64_t
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase,  ProtocolBaseMessage, ConnectionState

from unittest.mock import MagicMock
URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

cdef inline bint transport_receive(Transport transport, ProtocolBaseMessage **msg) nogil:
    cdef size_t msg_size = 0
    cdef void* transport_data = transport.receive(&msg_size)
    if transport_data == NULL:
        return 0

    if msg_size != sizeof(ProtocolBaseMessage):
        transport.receive_finalize(transport_data)
        return 0

    memcpy(msg[0], transport_data, msg_size)
    transport.receive_finalize(transport_data)
    return 1

class CyProtocolBaseTestCase(unittest.TestCase):

    def test_protocol_init(self):
        pass
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT , ZMQ_DEALER, b'CLI')

                ps = ProtocolBase(True, 11, transport_s)
                pc = ProtocolBase(False, 22, transport_c)

                assert ps.connections.item_size == sizeof(ConnectionState)

                assert ps.is_server == 1
                assert pc.is_server == 0

                assert ps.server_life_id > 11*10**8
                assert ps.client_life_id == 0

                assert pc.server_life_id == 0
                assert pc.client_life_id > 22*10**8
            except:
                raise
            finally:
                pass
                #transport_c.close()
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()

    def test_protocol_get_state_default(self):
        cdef ConnectionState *cstate;

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI')

                ps = ProtocolBase(True, 11, transport_s)
                pc = ProtocolBase(False, 22, transport_c)

                cstate = ps.get_state(b'TEST')
                assert cstate.sender_id == b'TEST'
                assert cstate.client_life_id == 0
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.status == ProtocolStatus.UHF_INACTIVE
                assert cstate.last_heartbeat_time_ns == 0
                assert cstate.msg_recvd == 0
                assert cstate.msg_sent == 0
                assert cstate.msg_errs == 0


                cstate = pc.get_state(b'')
                assert cstate.sender_id == b''
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.server_life_id == 0
                assert cstate.status == ProtocolStatus.UHF_INACTIVE
                assert cstate.last_heartbeat_time_ns == 0
                assert cstate.msg_recvd == 0
                assert cstate.msg_sent == 0
                assert cstate.msg_errs == 0

            except:
                raise
            finally:
                pass
                #transport_c.close()
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()

    def test_protocol_connect_sequence(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage*>malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', always_send_copy=True)

                ps = ProtocolBase(True, 11, transport_s)
                pc = ProtocolBase(False, 22, transport_c)

                #
                # Initial connection request
                #
                assert pc.connect() > 0
                # Client state after connection request
                cstate = pc.get_state(b'')
                assert cstate.server_life_id == 0
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.msg_sent == 1
                assert cstate.msg_recvd == 0
                assert cstate.msg_errs == 0
                assert cstate.status == ProtocolStatus.UHF_CONNECTING

                #
                # SERVER RECEIVES CONNECTION REQUEST FROM CLIENT
                #
                assert transport_receive(transport_s, &msg)

                # Check the incoming message validity
                assert msg.header.client_life_id == pc.client_life_id
                assert msg.header.server_life_id == 0
                assert msg.header.protocol_id == PROTOCOL_ID_BASE
                assert msg.header.sender_id == b'CLI'
                assert msg.header.msg_type == b'C'
                assert msg.header.magic_number == TRANSPORT_HDR_MGC
                assert msg.status == ProtocolStatus.UHF_CONNECTING

                assert ps.on_connect(msg) > 0
                sstate = ps.get_state(b'CLI')
                assert sstate != NULL
                assert sstate.client_life_id == pc.client_life_id
                assert sstate.server_life_id == ps.server_life_id
                assert sstate.msg_sent == 1
                assert sstate.msg_recvd == 1
                assert sstate.msg_errs == 0
                assert sstate.status == ProtocolStatus.UHF_CONNECTING

                #
                # CLIENT RECEIVES CONNECTION REPLY FROM THE SERVER
                #
                assert transport_receive(transport_c, &msg)
                # Check the incoming message validity
                assert msg.header.client_life_id == pc.client_life_id
                assert msg.header.server_life_id == ps.server_life_id
                assert msg.header.protocol_id == PROTOCOL_ID_BASE
                assert msg.header.sender_id == b'SRV'
                assert msg.header.msg_type == b'C'
                assert msg.header.magic_number == TRANSPORT_HDR_MGC
                assert msg.status == ProtocolStatus.UHF_CONNECTING

                assert pc.on_connect(msg) > 0
                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.status == ProtocolStatus.UHF_CONNECTING
                assert cstate.msg_errs == 0
                assert cstate.msg_sent == 1
                assert cstate.msg_recvd == 1

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)