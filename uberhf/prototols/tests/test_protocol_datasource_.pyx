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
from uberhf.prototols.protocol_datasource cimport ProtocolDataSource
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, InstrumentInfoStruct
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.includes.utils cimport datetime_nsnow, sleep_ns, timedelta_ns, TIMEDELTA_SEC, timer_nsnow, TIMEDELTA_MILLI
from uberhf.includes.hashmap cimport HashMap

from unittest.mock import MagicMock
URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'

cdef InstrumentInfoStruct global_iinfo
global_iinfo.tick_size = 10
global_iinfo.min_lot_size = 5
global_iinfo.margin_req = 100
global_iinfo.theo_price = 200
global_iinfo.price_scale = 2
global_iinfo.usd_point_value = 1

cdef class UHFeedMock(UHFeedAbstract):
    cdef ProtocolDataSource protocol
    cdef int on_initialize_ncalls
    cdef int on_disconnect_ncalls
    cdef int on_activate_ncalls

    cdef int on_register_n_ok
    cdef int on_register_n_err
    cdef size_t n_unique_tickers
    cdef int quotes_processed
    cdef HashMap hm_tickers

    def __cinit__(self):
        self.on_activate_ncalls = 0
        self.on_disconnect_ncalls = 0
        self.on_initialize_ncalls = 0
        self.on_register_n_ok = 0
        self.on_register_n_err = 0
        self.n_unique_tickers = 0
        self.quotes_processed = 0

        self.hm_tickers = HashMap(50)

    cdef void register_datasource_protocol(self, object protocol):
        self.protocol = <ProtocolDataSource> protocol


    cdef void source_on_initialize(self, char * source_id, unsigned int source_life_id) nogil:
        cyassert(strcmp(source_id, b'CLI') == 0)
        cyassert(source_life_id > 10L**8L)
        self.on_initialize_ncalls += 1

    cdef void source_on_activate(self, char * source_id) nogil:
        cyassert(strcmp(source_id, b'CLI') == 0)
        self.on_activate_ncalls += 1

    cdef void source_on_disconnect(self, char * source_id) nogil:
        cyassert(strcmp(source_id, b'CLI') == 0)
        self.on_disconnect_ncalls += 1


    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfoStruct * iinfo) nogil:
        cyassert(strcmp(source_id, b'CLI') == 0)

        if instrument_id == 4567:
            cyassert(strcmp(v2_ticker, b'RU.F.Si') == 0)
            self.on_register_n_err += 1
            return -1000
        else:
            cyassert(strcmp(v2_ticker, b'RU.F.RTS') == 0)
            cyassert(instrument_id == 1234)
            cyassert(iinfo.theo_price == 200)

            self.on_register_n_ok += 1
            self.hm_tickers.set(v2_ticker)
            self.n_unique_tickers = self.hm_tickers.count()
            # Mimic in mem index
            return self.n_unique_tickers

    cdef void source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil:
        cyassert(msg.instrument_id == 1234)
        cyassert(msg.instrument_index == 1)
        cyassert(msg.quote.bid == 100)
        cyassert(msg.quote.ask == 200)
        self.quotes_processed += 1

cdef class DataSourceMock(DatasourceAbstract):
    cdef ProtocolDataSource protocol
    cdef int on_initialize_ncalls
    cdef int on_disconnect_ncalls
    cdef int on_activate_ncalls
    cdef int on_register_n_ok
    cdef int on_register_n_err
    cdef size_t n_unique_tickers
    cdef HashMap hm_tickers
    cdef int quotes_sent

    def __cinit__(self):
        self.on_activate_ncalls = 0
        self.on_disconnect_ncalls = 0
        self.on_initialize_ncalls = 0
        self.on_register_n_ok = 0
        self.on_register_n_err = 0
        self.n_unique_tickers = 0
        self.quotes_sent = 0
        self.hm_tickers = HashMap(50)

    cdef void register_datasource_protocol(self, object protocol):
        self.protocol = <ProtocolDataSource> protocol

    cdef void source_on_initialize(self) nogil:
        self.on_initialize_ncalls += 1
        cyassert(self.protocol.send_register_instrument(b'RU.F.RTS', 1234, &global_iinfo) > 0)
        cyassert(self.protocol.send_register_instrument(b'RU.F.Si', 4567, &global_iinfo) > 0)
        cyassert(self.protocol.send_register_instrument(b'RU.F.RTS', 1234, &global_iinfo) > 0)
        cyassert(self.protocol.send_register_instrument(b'RU.F.RTS', 0, &global_iinfo) == PROTOCOL_ERR_ARG_ERR)
        cyassert(self.protocol.send_register_instrument(b'RU.F.sldfjasldfjslakjflksajflkjasfljsadldkfjsadlkfjswalkjflaskdjflksadjfdRTS', 22, &global_iinfo) == PROTOCOL_ERR_ARG_ERR)

    cdef void source_on_disconnect(self) nogil:
        self.on_disconnect_ncalls += 1

    cdef void source_on_activate(self) nogil:
        self.on_activate_ncalls += 1

        cdef ProtocolDSQuoteMessage msg
        msg.header.msg_type = b'q'
        msg.header.protocol_id = self.protocol.protocol_id
        msg.header.client_life_id = self.protocol.client_life_id
        msg.header.server_life_id = self.protocol.server_life_id

        msg.instrument_id = 1234
        msg.instrument_index = 1
        msg.is_snapshot = 0
        msg.quote.bid = 100
        msg.quote.ask = 200
        if self.protocol.send_new_quote(&msg, send_no_copy=0) > 0:
            self.quotes_sent += 1

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil:
        cdef int rc = 0
        if error_code == 0 and instrument_index >= 0:
            self.on_register_n_ok += 1
            cyassert(strcmp(v2_ticker, b'RU.F.RTS') == 0)
            cyassert(instrument_id == 1234)

            self.hm_tickers.set(v2_ticker)

            self.n_unique_tickers = self.hm_tickers.count()
            if self.on_register_n_ok == 2:
                return self.protocol.send_activate()
            else:
                return 1
        else:
            cyassert(strcmp(v2_ticker, b'RU.F.Si') == 0)
            cyassert(instrument_id == 4567)
            cyassert(error_code == -1000)
            self.on_register_n_err += 1
            # This is unexpected, but we need also check this
            return 0


class CyProtocolDataSourceBaseTestCase(unittest.TestCase):
    def test_protocol_sanity_checks(self):
        source = DataSourceMock()
        feed = UHFeedMock()

        #cybreakpoint(1)
        with self.assertRaises(ValueError) as exc:
            ps = ProtocolDataSource(11, None, None, None)
        self.assertEqual('You must set one of source_client or feed_server', str(exc.exception))

        with self.assertRaises(ValueError) as exc:
            ps = ProtocolDataSource(11, None, source, feed)
        self.assertEqual('Arguments are mutually exclusive: source_client, feed_server', str(exc.exception))

    def test_protocol_instrument_registration(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolBaseMessage *msg = <ProtocolBaseMessage *> malloc(sizeof(ProtocolBaseMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now

        cdef int heart_beat_call_cnt = 0

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                source = DataSourceMock()
                feed = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataSource(11, transport_s, None, feed)
                pc = ProtocolDataSource(22, transport_c, source, None)

                assert pc.send_register_instrument(b'TEST', 111, &global_iinfo) == PROTOCOL_ERR_WRONG_ORDER

                s_socket = zmq.Socket.shadow(<uint64_t> transport_s.socket)
                c_socket = zmq.Socket.shadow(<uint64_t> transport_c.socket)
                poller = zmq.Poller()
                poller.register(s_socket, zmq.POLLIN)
                poller.register(c_socket, zmq.POLLIN)

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
                        assert rc > 0 or rc == -1000, rc
                        heart_beat_call_cnt += 1
                    if c_socket in socks and socks[c_socket] == zmq.POLLIN:
                        transport_data = transport_c.receive(&msg_size)
                        rc = pc.on_process_new_message(transport_data, msg_size)
                        transport_c.receive_finalize(transport_data)
                        assert rc > 0 or rc == PROTOCOL_ERR_CLI_ERR, rc
                        heart_beat_call_cnt += 1

                    dt_now = datetime_nsnow()
                    if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:
                        assert ps.heartbeat(dt_now) >= 0
                        assert pc.heartbeat(dt_now) >= 0
                        heart_beat_call_cnt += 1

                        dt_prev_call = dt_now

                    if cstate.status == ProtocolStatus.UHF_ACTIVE and sstate.status == ProtocolStatus.UHF_ACTIVE:
                        assert pc.send_disconnect() >  0
                #
                # Check how core methods are called
                #
                assert source.on_initialize_ncalls == 1
                assert feed.on_initialize_ncalls == 1

                assert feed.on_register_n_ok == 2, heart_beat_call_cnt
                assert feed.on_register_n_err == 1
                assert feed.n_unique_tickers == 1

                assert source.on_register_n_err == 1
                assert source.on_register_n_ok == 2, int(source.on_register_n_ok)
                assert source.n_unique_tickers == 1

                assert source.on_activate_ncalls == 1
                assert feed.on_activate_ncalls == 1

                assert source.on_disconnect_ncalls == 1
                assert feed.on_disconnect_ncalls == 1

                assert source.quotes_sent == 1
                assert feed.quotes_processed == 1

                cstate = pc.get_state(b'')
                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                sstate = ps.get_state(b'CLI')
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)


    def test_protocol_message_processing_errors(self):
        cdef ConnectionState *cstate;
        cdef ConnectionState *sstate;
        cdef ProtocolDSRegisterMessage *msg = <ProtocolDSRegisterMessage *> malloc(sizeof(ProtocolDSRegisterMessage))
        cdef void * transport_data
        cdef size_t msg_size
        cdef long dt_prev_call
        cdef long dt_now

        with zmq.Context() as ctx:
            transport_s = None
            transport_c = None
            try:
                transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV', always_send_copy=True)
                transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_DEALER, b'CLI', router_id=b'SRV', always_send_copy=True)
                time.sleep(0.1)  # Sleep to make connection established, because transport_c is non-blocking!

                source = DataSourceMock()
                feed = UHFeedMock()

                #cybreakpoint(1)
                ps = ProtocolDataSource(11, transport_s, None, feed)
                pc = ProtocolDataSource(22, transport_c, source, None)
                cstate = pc.get_state(b'')
                sstate = ps.get_state(b'CLI')
                assert cstate.status == ProtocolStatus.UHF_INACTIVE, int(cstate.status)
                assert sstate.status == ProtocolStatus.UHF_INACTIVE, int(sstate.status)

                msg.header.protocol_id = b'T'
                assert pc.on_process_new_message(msg, sizeof(ProtocolDSRegisterMessage)) == 0

                msg.header.protocol_id = b'S'
                msg.header.msg_type = b'r'
                assert pc.on_process_new_message(msg, sizeof(ProtocolDSRegisterMessage)-1) == PROTOCOL_ERR_SIZE


                msg.header.protocol_id = b'S'
                msg.header.msg_type = b'q'
                assert pc.on_process_new_message(msg, sizeof(ProtocolDSQuoteMessage)-1) == PROTOCOL_ERR_SIZE


            except:
                raise
            finally:
                if transport_s:
                    transport_s.close()
                if transport_c:
                    transport_c.close()
                free(msg)
