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
from uberhf.prototols.messages cimport *
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.includes.utils cimport datetime_nsnow, strlcpy, timedelta_ns, TIMEDELTA_SEC, timer_nsnow, TIMEDELTA_MILLI
from uberhf.includes.hashmap cimport HashMap

URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

URL_BIND_PUB = b'tcp://*:7101'
URL_CONNECT_SUB = b'tcp://localhost:7101'


cdef class UHFeedMock(UHFeedAbstract):
    cdef ProtocolDataFeed protocol
    cdef HashMap hm_tickers
    cdef int n_subs
    cdef int n_unsubs
    cdef int n_errors
    cdef int n_tickers

    def __cinit__(self):
        self.hm_tickers = HashMap(50)
        self.n_subs = 0
        self.n_unsubs = 0
        self.n_errors = 0
        self.n_tickers = 0

    cdef void register_datafeed_protocol(self, object protocol):
        self.protocol = <ProtocolDataFeed> protocol

    cdef int feed_on_subscribe(self, char * v2_ticker, unsigned int client_life_id, bint is_subscribe) nogil:
        cyassert(client_life_id > 10**9)
        if is_subscribe:
            if strcmp(v2_ticker, b'RU.F.Si') == 0:
                self.n_errors += 1
                return -1
            self.hm_tickers.set(v2_ticker)
            self.n_subs += 1
        else:
            self.hm_tickers.delete(v2_ticker)
            self.n_unsubs += 1

        self.n_tickers = self.hm_tickers.count()
        return self.n_tickers

cdef class FeedClientMock(FeedClientAbstract):
    cdef ProtocolDataFeed protocol
    cdef HashMap hm_tickers
    cdef int n_subs
    cdef int n_unsubs
    cdef int n_errors
    cdef int n_tickers
    cdef int n_on_status
    cdef int n_quotes_upd
    cdef int n_iinfo_upd

    def __cinit__(self):
        self.hm_tickers = HashMap(50)
        self.n_subs = 0
        self.n_unsubs = 0
        self.n_errors = 0
        self.n_tickers = 0
        self.n_on_status = 0
        self.n_quotes_upd = 0
        self.n_iinfo_upd = 0

    cdef void register_datafeed_protocol(self, object protocol):
        self.protocol = <ProtocolDataFeed> protocol

    cdef void feed_on_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil:
        if instrument_index < 0:
            self.n_errors += 1

        if is_subscribe:

            if instrument_index >= 0:
                #cyassert(self.hm_tickers.get(v2_ticker) == NULL)
                self.hm_tickers.set(v2_ticker)
                self.n_subs += 1
        else:
            #cyassert(self.hm_tickers.get(v2_ticker) != NULL)
            self.hm_tickers.delete(v2_ticker)
            self.n_unsubs += 1

        self.n_tickers = self.hm_tickers.count()

    cdef void feed_on_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil:
        cyassert(strcmp(b'SRC1', data_source_id) == 0)
        cyassert(quotes_status == ProtocolStatus.UHF_ACTIVE)
        self.n_on_status += 1

    cdef void feed_on_quote(self, int instrument_index) nogil:
        cyassert(instrument_index == 17)
        self.n_quotes_upd += 1

    cdef void feed_on_instrumentinfo(self, int instrument_index) nogil:
        cyassert(instrument_index == 17)
        self.n_iinfo_upd += 1


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

    def test_protocol_transport_name_must_be_equal(self):
        with zmq.Context() as ctx:
            transport_s = None
            transport_s_pub = None
            try:
                feed_client = FeedClientMock()
                feed_server = UHFeedMock()

                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'DIFF', always_send_copy=True)

                with self.assertRaises(ValueError) as exc:
                    ps = ProtocolDataFeed(11, transport_s, transport_s_pub, None, feed_server)
                self.assertEqual(str(exc.exception), 'Both transports must have identical `transport_id`')
            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_s_pub:
                    transport_s_pub.close()

    def test_protocol_subscription(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now
        was_active = False
        was_subscribed = False
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            transport_c_sub = None
            transport_s_pub = None
            try:
                feed_client = FeedClientMock()
                feed_server = UHFeedMock()


                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

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

                assert pc.send_subscribe(b'ABSD') == PROTOCOL_ERR_WRONG_ORDER

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
                        if not was_active:
                            was_active = True
                            assert pc.send_subscribe(b'') == PROTOCOL_ERR_ARG_ERR
                            assert pc.send_subscribe(b'RU.F.RTS') > 0
                            assert pc.send_subscribe(b'RU.F.RTS') > 0
                            assert pc.send_subscribe(b'RU.F.Si') > 0
                            was_subscribed = True
                        elif was_subscribed:
                            assert pc.send_unsubscribe(b'RU.F.RTS') > 0
                            assert pc.send_unsubscribe(b'RU.F.RTS') > 0
                            assert pc.send_unsubscribe(b'RU.F.Si') > 0
                            was_subscribed = False

                        #assert pc.send_disconnect() >  0
                #
                # Check how core methods are called
                #
                assert was_active
                assert cstate.status == ProtocolStatus.UHF_ACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_ACTIVE, int(sstate.status)


                assert feed_server.n_subs == 2
                assert feed_client.n_subs == 2
                assert feed_server.n_unsubs == 3, feed_server.n_unsubs
                assert feed_client.n_unsubs == 3, feed_client.n_unsubs
                assert feed_server.n_errors == 1
                assert feed_client.n_errors == 1
                assert feed_client.n_tickers == 0, feed_client.n_tickers
                assert feed_server.n_tickers == 0, feed_server.n_tickers
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


    def test_protocol_connection(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now
        was_active = False
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            transport_c_sub = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

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
                        was_active = True

                        assert pc.send_disconnect() >  0
                #
                # Check how core methods are called
                #
                assert was_active
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


    def test_protocol_source_status(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now
        was_active = False
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            transport_c_sub = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

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
                        was_active = True
                        assert ps.send_source_status(NULL, ProtocolStatus.UHF_ACTIVE) == PROTOCOL_ERR_ARG_ERR
                        self.assertGreater(ps.send_source_status(b'SRC1', ProtocolStatus.UHF_ACTIVE), 0)
                        assert pc.send_disconnect() >  0
                #
                # Check how core methods are called
                #
                assert was_active
                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

                assert feed_client.n_on_status == 1
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

    def test_protocol_updates(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now
        was_active = False
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            transport_c_sub = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

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
                        #self.assertGreater(ps.send_source_status(b'SRC1', ProtocolStatus.UHF_ACTIVE), 0)
                        assert ps.send_feed_update(-1, 1, 2) == PROTOCOL_ERR_ARG_ERR
                        assert ps.send_feed_update(0, 0, 2) == PROTOCOL_ERR_ARG_ERR
                        assert ps.send_feed_update(0, 3, 2) == PROTOCOL_ERR_ARG_ERR

                        # Setting bit in decimal for pc.module_id=22 -> 2**22 -> 22th bit is set to true
                        self.assertGreater(ps.send_feed_update(17, 1, 2**22), 0)
                        self.assertGreater(ps.send_feed_update(17, 2, 2**22), 0)
                        assert pc.send_disconnect() > 0
                        was_active = True

                #
                # Check how core methods are called
                #
                assert was_active
                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

                assert feed_client.n_quotes_upd == 1
                assert feed_client.n_iinfo_upd == 1
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


    def test_protocol_updates_subscription_bits_were_filtered(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now
        was_active = False
        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            transport_c_sub = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

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
                        #self.assertGreater(ps.send_source_status(b'SRC1', ProtocolStatus.UHF_ACTIVE), 0)
                        assert ps.send_feed_update(-1, 1, 2) == PROTOCOL_ERR_ARG_ERR
                        assert ps.send_feed_update(0, 0, 2) == PROTOCOL_ERR_ARG_ERR
                        assert ps.send_feed_update(0, 3, 2) == PROTOCOL_ERR_ARG_ERR

                        # settings different subscription bits, it's not an error but the
                        # quote would not be passed to the client processing
                        self.assertGreater(ps.send_feed_update(17, 1, 2**11), 0)
                        self.assertGreater(ps.send_feed_update(17, 2, 2**1), 0)
                        assert pc.send_disconnect() > 0
                        was_active = True

                #
                # Check how core methods are called
                #
                assert was_active
                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

                assert feed_client.n_quotes_upd == 0
                assert feed_client.n_iinfo_upd == 0
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


    def test_protocol_on_process_new_message_errors__msgupdate(self):
        cdef ProtocolDFUpdateMessage *msg = <ProtocolDFUpdateMessage *> malloc(sizeof(ProtocolDFUpdateMessage))


        with zmq.Context() as ctx:
            transport_s = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                feed_client = FeedClientMock()
                feed_server = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataFeed(11, transport_s, transport_s_pub, None, feed_server)
                pc = ProtocolDataFeed(22, transport_c, transport_c_sub, feed_client, None)


                msg.header.msg_type = b'u'
                msg.header.server_life_id = ps.server_life_id
                msg.header.client_life_id = TRANSPORT_HDR_MGC
                msg.instrument_index = 0
                msg.update_type = 1
                msg.header.protocol_id = b'T'
                msg.subscriptions_bits = 2**22
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage) - 1) == 0

                msg.header.protocol_id = ps.protocol_id

                # Subscription is filtered
                msg.subscriptions_bits = 0
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)) == 10000

                msg.subscriptions_bits = 2 ** 22
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)-1) == PROTOCOL_ERR_SIZE

                msg.header.client_life_id = 0
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)) == PROTOCOL_ERR_LIFE_ID


                msg.header.client_life_id = TRANSPORT_HDR_MGC
                msg.update_type = 0
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)) == PROTOCOL_ERR_WRONG_TYPE
                msg.update_type = -1
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)) == PROTOCOL_ERR_WRONG_TYPE
                msg.update_type = 3
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFUpdateMessage)) == PROTOCOL_ERR_WRONG_TYPE

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



    def test_protocol_on_process_new_message_errors__msgsubscr(self):
        cdef ProtocolDFSubscribeMessage *msg = <ProtocolDFSubscribeMessage *> malloc(sizeof(ProtocolDFSubscribeMessage))


        with zmq.Context() as ctx:
            transport_s = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                feed_client = FeedClientMock()
                feed_server = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataFeed(11, transport_s, transport_s_pub, None, feed_server)
                pc = ProtocolDataFeed(22, transport_c, transport_c_sub, feed_client, None)

                msg.header.protocol_id = ps.protocol_id
                msg.header.msg_type = b's'
                msg.header.server_life_id = ps.server_life_id
                msg.header.client_life_id = TRANSPORT_HDR_MGC
                strlcpy(msg.v2_ticker, b'RTSE', 10)
                msg.is_subscribe = 1
                msg.instrument_index = -1

                assert pc.on_process_new_message(msg, sizeof(ProtocolDFSubscribeMessage)-1) == PROTOCOL_ERR_SIZE

                assert pc.on_process_new_message(msg, sizeof(ProtocolDFSubscribeMessage)) == 1

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


    def test_protocol_on_process_new_message_errors__msgstatus(self):
        cdef ProtocolDFStatusMessage *msg = <ProtocolDFStatusMessage *> malloc(sizeof(ProtocolDFStatusMessage))


        with zmq.Context() as ctx:
            transport_s = None
            transport_s_pub = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)

                transport_s_pub = Transport(<uint64_t> ctx.underlying, URL_BIND_PUB, ZMQ_PUB, b'SRV', always_send_copy=True)
                transport_c_sub = Transport(<uint64_t> ctx.underlying, URL_CONNECT_SUB, ZMQ_SUB, b'CLI', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                feed_client = FeedClientMock()
                feed_server = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataFeed(11, transport_s, transport_s_pub, None, feed_server)
                pc = ProtocolDataFeed(22, transport_c, transport_c_sub, feed_client, None)

                msg.header.protocol_id = ps.protocol_id
                msg.header.msg_type = b'c'
                msg.header.server_life_id = ps.server_life_id
                msg.header.client_life_id = TRANSPORT_HDR_MGC
                strlcpy(msg.data_source_id, b'123', 5)
                msg.quote_status = ProtocolStatus.UHF_ACTIVE

                assert pc.on_process_new_message(msg, sizeof(ProtocolDFStatusMessage)-1) == PROTOCOL_ERR_SIZE

                msg.header.client_life_id = 0
                assert pc.on_process_new_message(msg, sizeof(ProtocolDFStatusMessage)) == PROTOCOL_ERR_LIFE_ID

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

