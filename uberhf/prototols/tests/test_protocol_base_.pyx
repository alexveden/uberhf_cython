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
                assert cstate.last_msg_time_ns == 0
                assert cstate.msg_recvd == 0
                assert cstate.msg_sent == 0
                assert cstate.msg_errs == 0


                cstate = pc.get_state(b'')
                assert cstate.sender_id == b''
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.server_life_id == 0
                assert cstate.status == ProtocolStatus.UHF_INACTIVE
                assert cstate.last_msg_time_ns == 0
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
                assert pc.send_connect() > 0
                # Client state after connection request
                cstate = pc.get_state(b'')
                assert cstate.server_life_id == 0
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.msg_sent == 1
                assert cstate.msg_recvd == 0
                assert cstate.msg_errs == 0
                assert cstate.status == ProtocolStatus.UHF_INACTIVE

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
                assert cstate.msg_sent == 2
                assert cstate.msg_recvd == 1

                # Client sends immediately activate command!

                assert transport_receive(transport_s, &msg)

                # Check the incoming message validity
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.client_life_id == pc.client_life_id
                assert msg.header.protocol_id == PROTOCOL_ID_BASE
                assert msg.header.sender_id == b'CLI'
                assert msg.header.msg_type == b'A'
                assert msg.header.magic_number == TRANSPORT_HDR_MGC
                assert msg.status == ProtocolStatus.UHF_ACTIVE

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)

    def test_protocol_activate_sequence(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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
                assert pc.send_connect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_connect(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_connect(msg) > 0
                #
                # Activating client
                #
                # This will be sent automatically pc.on_connect()
                #assert pc.send_activate() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_activate(msg) > 0

                sstate = ps.get_state(b'CLI')
                assert sstate != NULL
                assert sstate.client_life_id == pc.client_life_id
                assert sstate.server_life_id == ps.server_life_id
                assert sstate.msg_sent == 2
                assert sstate.msg_recvd == 2
                assert sstate.msg_errs == 0
                assert sstate.status == ProtocolStatus.UHF_ACTIVE

                assert transport_receive(transport_c, &msg)
                assert pc.on_activate(msg) > 0

                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.status == ProtocolStatus.UHF_ACTIVE
                assert cstate.msg_errs == 0
                assert cstate.msg_sent == 2
                assert cstate.msg_recvd == 2

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)


    def test_protocol_disconnect_sequence_from_active(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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
                assert pc.send_connect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_connect(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_connect(msg) > 0
                #assert pc.send_activate() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_activate(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_activate(msg) > 0

                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.status == ProtocolStatus.UHF_ACTIVE
                assert cstate.msg_errs == 0
                assert cstate.msg_sent == 2
                assert cstate.msg_recvd == 2

                assert pc.send_disconnect() > 0
                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.server_life_id == 0
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.status == ProtocolStatus.UHF_INACTIVE
                assert cstate.msg_errs == 0
                assert cstate.msg_sent == 3
                assert cstate.msg_recvd == 2

                assert transport_receive(transport_s, &msg)
                assert ps.on_disconnect(msg) > 0
                sstate = ps.get_state(b'CLI')
                assert sstate != NULL
                assert sstate.client_life_id == 0
                assert sstate.server_life_id == ps.server_life_id
                assert sstate.msg_sent == 2
                assert sstate.msg_recvd == 3
                assert sstate.msg_errs == 0
                assert sstate.status == ProtocolStatus.UHF_INACTIVE

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)

    def test_protocol_disconnect_sequence_from_connecting(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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
                assert pc.send_connect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_connect(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_connect(msg) > 0

                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.server_life_id == ps.server_life_id
                assert cstate.client_life_id == pc.client_life_id
                assert cstate.status == ProtocolStatus.UHF_CONNECTING

                assert pc.send_disconnect() > 0
                cstate = pc.get_state(b'')
                assert cstate != NULL
                assert cstate.status == ProtocolStatus.UHF_INACTIVE

                assert transport_receive(transport_s, &msg)
                assert ps.on_disconnect(msg) > 0
                sstate = ps.get_state(b'CLI')
                assert sstate != NULL
                assert sstate.status == ProtocolStatus.UHF_INACTIVE

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)

    def test_protocol_status_transition(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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

                assert ps._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_CONNECTING
                assert pc._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_CONNECTING
                assert ps._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INACTIVE, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE


                assert ps._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INITIALIZING
                assert pc._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INITIALIZING
                assert ps._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_CONNECTING, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE

                assert ps._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INITIALIZING
                assert pc._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INITIALIZING
                assert ps._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_INITIALIZING, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE


                assert ps._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_CONNECTING) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_INITIALIZING) == ProtocolStatus.UHF_INACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_ACTIVE) == ProtocolStatus.UHF_ACTIVE
                assert ps._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE
                assert pc._state_transition(ProtocolStatus.UHF_ACTIVE, ProtocolStatus.UHF_INACTIVE) == ProtocolStatus.UHF_INACTIVE

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)


    def test_protocol_heartbeat_sequence(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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
                assert pc.send_connect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_connect(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_connect(msg) > 0
                #
                # Activating client
                #
                assert pc.send_activate() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_activate(msg) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_activate(msg) > 0

                cstate = pc.get_state(b'')
                assert cstate.status == ProtocolStatus.UHF_ACTIVE
                #
                # Sending heartbeat
                #
                assert pc.send_heartbeat() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_heartbeat(msg) > 0

                sstate = ps.get_state(b'CLI')
                assert sstate.n_heartbeats == 1


                assert transport_receive(transport_c, &msg)
                assert pc.on_heartbeat(msg) > 0
                cstate = pc.get_state(b'')
                assert cstate.n_heartbeats == 1


            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)


    def test_protocol_message_processing(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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
                assert pc.send_connect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                #
                # Activating client
                #
                #assert pc.send_activate() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0

                cstate = pc.get_state(b'')
                assert cstate.status == ProtocolStatus.UHF_ACTIVE
                #
                # Sending heartbeat
                #
                assert pc.send_heartbeat() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0

                sstate = ps.get_state(b'CLI')
                assert sstate.n_heartbeats == 1


                assert transport_receive(transport_c, &msg)
                assert pc.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                cstate = pc.get_state(b'')
                assert cstate.n_heartbeats == 1

                assert pc.send_disconnect() > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0

                # Size mismatch
                assert ps.on_process_new_message(b'', 0) == 0

                # Protocol ID match but incorrect message type
                msg.header.msg_type = b'T'
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) == PROTOCOL_ERR_WRONG_TYPE

                # Protocol ID mismatch must message mismatch
                msg.header.protocol_id = b'T'
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) == 0
            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)

    def test_protocol_connection_via_heartbeat(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
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


                assert pc.heartbeat(datetime_nsnow()) >= 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                assert transport_receive(transport_s, &msg)
                assert ps.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0
                assert transport_receive(transport_c, &msg)
                assert pc.on_process_new_message(msg, sizeof(ProtocolBaseMessage)) > 0


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

                ps = ProtocolBase(True, 11, transport_s)
                pc = ProtocolBase(False, 22, transport_c)

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

    def test_protocol_connection_via_poller__no_server(self):
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
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', always_send_copy=True)

                pc = ProtocolBase(False, 22, transport_c, heartbeat_interval_sec=0.05)

                c_socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                poller = zmq.Poller()
                poller.register(c_socket, zmq.POLLIN)

                dt_prev_call = datetime_nsnow()
                cstate = pc.get_state(b'')
                assert cstate.last_msg_time_ns == 0

                for i in range(10):
                    socks = dict(poller.poll(50))
                    if c_socket in socks and socks[c_socket] == zmq.POLLIN:
                        transport_data = transport_c.receive(&msg_size)
                        assert pc.on_process_new_message(transport_data, msg_size) > 0
                        transport_c.receive_finalize(transport_data)

                    dt_now = datetime_nsnow()
                    if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:
                        hb = pc.heartbeat(dt_now)

                        if i == 0:
                            # Trying to connect
                            assert hb > 0
                            assert cstate.msg_sent == 1
                            assert cstate.last_msg_time_ns > 0
                            assert cstate.status == ProtocolStatus.UHF_INACTIVE

                        elif i == 1:
                            # Timeout should work!
                            assert cstate.msg_sent == 1
                            assert hb == 0

                        dt_prev_call = dt_now

                self.assertEqual(cstate.msg_sent, 4)
                assert cstate.status == ProtocolStatus.UHF_INACTIVE

            except:
                raise
            finally:
                if transport_c:
                    transport_c.close()
                free(msg)

    def test_protocol_connection_via_poller_timeout(self):
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

                ps = ProtocolBase(True, 11, transport_s, heartbeat_interval_sec=0.05)
                pc = ProtocolBase(False, 22, transport_c, heartbeat_interval_sec=0.05)

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

                for i in range(10):
                    socks = dict(poller.poll(50))
                    if s_socket in socks and socks[s_socket] == zmq.POLLIN:
                        transport_data = transport_s.receive(&msg_size)
                        # Send malformed message size to make timeouts
                        assert ps.on_process_new_message(transport_data, msg_size - 1) == 0
                        transport_s.receive_finalize(transport_data)
                    if c_socket in socks and socks[c_socket] == zmq.POLLIN:
                        transport_data = transport_c.receive(&msg_size)
                        # Send malformed message size to make timeouts
                        assert pc.on_process_new_message(transport_data, msg_size - 1) == 0
                        transport_c.receive_finalize(transport_data)

                    dt_now = datetime_nsnow()
                    if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:
                        hbs = ps.heartbeat(dt_now)
                        hbc = pc.heartbeat(dt_now)
                        if hbc < 0:
                            assert hbc == PROTOCOL_ERR_SRV_TIMEO, hbc
                        if hbs < 0:
                            assert  hbs == PROTOCOL_ERR_CLI_TIMEO, hbs
                        dt_prev_call = dt_now

                cstate = pc.get_state(b'')
                assert cstate.status == ProtocolStatus.UHF_INACTIVE
                assert cstate.server_life_id == 0
                sstate = ps.get_state(b'CLI')
                assert sstate.status == ProtocolStatus.UHF_INACTIVE
                assert sstate.client_life_id == 0

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)
