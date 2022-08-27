import time
from libc.stdlib cimport malloc, free
from libc.string cimport strcmp
import unittest
import zmq
from libc.stdint cimport uint64_t
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.prototols.protocol_base cimport ProtocolBase,  ProtocolBaseMessage, ConnectionState
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.includes.utils cimport datetime_nsnow, sleep_ns, timedelta_ns, TIMEDELTA_SEC, timer_nsnow, TIMEDELTA_MILLI
from uberhf.includes.hashmap cimport HashMap

URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

URL_BIND_PUB = b'tcp://*:7101'
URL_CONNECT_SUB = b'tcp://localhost:7101'


cdef class UHFeedMock(UHFeedAbstract):
    cdef ProtocolDataFeed protocol
    cdef HashMap hm_tickers

    def __cinit__(self):
        self.hm_tickers = HashMap(50)

    cdef void register_datafeed_protocol(self, object protocol):
        self.protocol = <ProtocolDataFeed> protocol

cdef class FeedClientMock(FeedClientAbstract):
    cdef ProtocolDataFeed protocol
    cdef HashMap hm_tickers

    def __cinit__(self):
        self.hm_tickers = HashMap(50)

    cdef void register_datafeed_protocol(self, object protocol):
        self.protocol = <ProtocolDataFeed> protocol

class CyProtocolDataFeedBaseTestCase(unittest.TestCase):
    def test_protocol_sanity_checks(self):
        source = object()
        feed = object()

        #cybreakpoint(1)
        with self.assertRaises(ValueError) as exc:
            ps = ProtocolDataFeed(11, None, None, None, None)
        self.assertEqual('You must set one of feed_client or feed_server', str(exc.exception))

        with self.assertRaises(ValueError) as exc:
            ps = ProtocolDataFeed(11, None, None, source, feed)
        self.assertEqual('Arguments are mutually exclusive: feed_client, feed_server', str(exc.exception))


    def test_protocol_connection(self):
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
            transport_c_sub = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)

                feed_client = FeedClientMock()
                feed_server = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataFeed(11, transport_s, transport_s_pub, None, feed_server)
                pc = ProtocolDataFeed(22, transport_c, transport_c_sub, feed_client, None)

                s_socket = zmq.Socket.shadow(<uint64_t> transport_s.socket)
                c_socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                s_pub_socket = zmq.Socket.shadow(<uint64_t> transport_s_pub.socket)
                c_sub_socket = zmq.Socket.shadow(<uint64_t> transport_c_sub.socket)
                poller = zmq.Poller()
                poller.register(s_socket, zmq.POLLIN)
                poller.register(c_socket, zmq.POLLIN)
                #poller.register(s_pub_socket, zmq.POLLIN)
                poller.register(c_sub_socket, zmq.POLLIN)

                cstate = pc.get_state(b'')
                sstate = ps.get_state(b'CLI')

                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

                dt_prev_call = datetime_nsnow()
                for i in range(20):
                    socks = dict(poller.poll(50))
                    if s_socket in socks and socks[s_socket] == zmq.POLLIN:
                        transport_data = transport_s.receive(&msg_size)
                        rc = ps.on_process_new_message(transport_data, msg_size)
                        transport_s.receive_finalize(transport_data)
                        assert rc > 0, rc
                    if c_socket in socks and socks[c_socket] == zmq.POLLIN:
                        transport_data = transport_c.receive(&msg_size)
                        rc = pc.on_process_new_message(transport_data, msg_size)
                        transport_c.receive_finalize(transport_data)
                        assert rc > 0, rc
                    if c_sub_socket in socks and socks[c_sub_socket] == zmq.POLLIN:
                        transport_data = transport_c_sub.receive(&msg_size)
                        rc = pc.on_process_new_message(transport_data, msg_size)
                        transport_c_sub.receive_finalize(transport_data)
                        assert rc > 0, rc

                    dt_now = datetime_nsnow()
                    if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:
                        assert ps.heartbeat(dt_now) >= 0
                        assert pc.heartbeat(dt_now) >= 0

                        dt_prev_call = dt_now

                    if cstate.status == ProtocolStatus.UHF_ACTIVE and sstate.status == ProtocolStatus.UHF_ACTIVE:
                        assert pc.send_disconnect() >  0
                #
                # Check how core methods are called
                #

                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)
            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                if transport_s_pub:
                    transport_s_pub.close()
                if transport_c_sub:
                    transport_c_sub.close()
                free(msg)

