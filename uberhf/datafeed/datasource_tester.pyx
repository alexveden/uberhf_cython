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
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, InstrumentInfo
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.includes.utils cimport gen_lifetime_id, datetime_nsnow, TIMEDELTA_MILLI, timedelta_ns, strlcpy, random_int, TIMEDELTA_SEC, sleep_ns
from uberhf.includes.hashmap cimport HashMap
from .quotes_cache cimport SharedQuotesCache
import numpy as np
cimport numpy as np
from libc.stdio cimport printf

cdef InstrumentInfo global_iinfo
global_iinfo.tick_size = 10
global_iinfo.min_lot_size = 5
global_iinfo.margin_req = 100
global_iinfo.theo_price = 200
global_iinfo.price_scale = 2
global_iinfo.usd_point_value = 1

cdef extern from "<signal.h>" nogil:
    enum: SIGINT
    enum: SIGQUIT
    int signal(int signum, void (*sighandler_t)(int))

cdef void sig_handler(int signum) nogil:
    """
    Simple signal handler to stop the program by Ctrl+C
    """
    global global_is_shutting_down
    global_is_shutting_down = 1

cdef char * make_ticker(letter_arr):
    cdef char * buf = <char*>malloc(sizeof(char) * len(letter_arr) + 1)

    for i in range(len(letter_arr)):
        buf[i] = <char>letter_arr[i]
    buf[len(letter_arr)] = b'\0'
    return buf

ctypedef struct SourceTickerCache:
    char v2_ticker[V2_TICKER_MAX_LEN]
    ProtocolDSQuoteMessage qmsg


cdef class DataSourceTester(DatasourceAbstract):
    def __cinit__(self, zmq_context_ptr, zmq_url_dealer, n_unique_tickers):
        self.transport_dealer = None
        self.transport_dealer = Transport(<uint64_t> zmq_context_ptr, zmq_url_dealer, ZMQ_DEALER, b'DSTST')
        self.protocol = ProtocolDataSource(MODULE_ID_TEST_SRC, self.transport_dealer, self, None)
        self.zmq_poll_timeout = 50
        self.zmq_poll_array[0] = [self.transport_dealer.socket, 0, ZMQ_POLLIN, 0]
        self.is_shutting_down = 0

        #
        # Init ticker cache
        #
        self.on_activate_ncalls = 0
        self.on_disconnect_ncalls = 0
        self.on_initialize_ncalls = 0
        self.on_register_n_ok = 0
        self.on_register_n_err = 0
        self.n_unique_tickers = n_unique_tickers
        self.quotes_sent = 0
        self.quotes_sent_errors = 0
        self.hm_tickers = HashMap(sizeof(SourceTickerCache), n_unique_tickers)
        ticker_arr = np.random.choice(np.array([l for l in 'ABCDEFGHJKLMNOPQRSTWXYZ'.encode()]), size=(n_unique_tickers, 10))
        assert len(ticker_arr) == n_unique_tickers
        cdef SourceTickerCache tc
        cdef char * ticker
        for i, s in enumerate(ticker_arr):
            ticker = make_ticker(ticker_arr[i])
            strlcpy(tc.v2_ticker, ticker, V2_TICKER_MAX_LEN)
            tc.qmsg.header.magic_number = TRANSPORT_HDR_MGC
            tc.qmsg.header.msg_type = b'q'

            strlcpy(tc.qmsg.header.sender_id, self.transport_dealer.transport_id, TRANSPORT_SENDER_SIZE)

            tc.qmsg.header.protocol_id = self.protocol.protocol_id
            tc.qmsg.instrument_index = -1 # This index will be filled at source_on_register_instrument()
            tc.qmsg.is_snapshot = 1
            tc.qmsg.instrument_id = (i+1) + 10**7

            assert tc.qmsg.instrument_id > 0

            SharedQuotesCache.reset_quote(&tc.qmsg.quote)
            self.hm_tickers.set(&tc)

        assert self.hm_tickers.count() == n_unique_tickers
        print(f'#{self.hm_tickers.count()} unique random tickers were generated')

    def __dealloc__(self):
        # Memory cleanup
        if self.transport_dealer is not None:
            self.transport_dealer.close()

    cdef void register_datasource_protocol(self, object protocol):
        self.protocol = <ProtocolDataSource> protocol

    cdef void source_on_initialize(self) nogil:
        printf(b'source_on_initialize: sending registration\n')
        cdef int rc = 0
        self.on_initialize_ncalls += 1
        self.on_register_n_err = 0
        self.on_register_n_ok = 0

        cdef size_t i = 0
        cdef void * hm_data = NULL
        cdef SourceTickerCache * tc
        cdef int n_sent = 0

        while self.hm_tickers.iter(&i, &hm_data):
            tc = <SourceTickerCache *> hm_data
            rc = self.protocol.send_register_instrument(tc.v2_ticker, tc.qmsg.instrument_id, &global_iinfo)
            if rc < 0:
                printf(b'source_on_initialize: send_register_instrument Error %d -> v2_ticker: %s instrument_id: %u\n', rc, tc.v2_ticker, tc.qmsg.instrument_id)
            else:
                n_sent += 1

        printf(b'source_on_initialize: %d instruments registered\n', n_sent)

    cdef void benchmark_quotes(self, int n_quotes) nogil:

        printf('benchmarking %d quotes\n', n_quotes)
        cdef SourceTickerCache ** tc_array = <SourceTickerCache **>malloc(sizeof(SourceTickerCache*) * self.n_unique_tickers)
        cyassert(tc_array != NULL)
        cdef size_t i = 0
        cdef int j = 0
        cdef void * hm_data = NULL
        cdef SourceTickerCache * tc

        cyassert(self.hm_tickers.count() == self.n_unique_tickers)

        while self.hm_tickers.iter(&i, &hm_data):
            tc_array[j] = <SourceTickerCache *> hm_data
            j += 1

        cdef long dt_now = datetime_nsnow()
        for i in range(n_quotes):
            j = random_int(0, self.n_unique_tickers)
            if self.protocol.send_new_quote(&tc_array[j].qmsg, send_no_copy=-1) > 0:
                self.quotes_sent += 1
            else:
                self.quotes_sent_errors += 1
                if self.quotes_sent_errors == 1:
                    printf('First error occurred in %0.6fsec\n', timedelta_ns(datetime_nsnow(), dt_now, TIMEDELTA_SEC))

        cdef double duration = timedelta_ns(datetime_nsnow(), dt_now, TIMEDELTA_SEC)
        cdef double speed = n_quotes / duration
        #free(tc_array)
        printf('Completed in %0.6fsec %0.1f quotes/sec\n', duration, speed)



    cdef void source_on_disconnect(self) nogil:
        self.on_disconnect_ncalls += 1
        printf(b'source_on_disconnect: disconnected\n')

    cdef void source_on_activate(self) nogil:
        self.on_activate_ncalls += 1
        printf(b'source_on_activate: active #%d registered, #%d reg errs\n', self.on_register_n_ok, self.on_register_n_err)

        printf(b'source_on_activate: benchmark quotes in 5 sec\n')
        sleep_ns(5)
        self.benchmark_quotes(1000000)
        printf(b'source_on_activate: benchmark quotes done\n')

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil:
        cdef SourceTickerCache * tc = <SourceTickerCache *>self.hm_tickers.get(v2_ticker)

        if tc == NULL:
            self.on_register_n_err += 1
            return -1
        cyassert(tc.qmsg.instrument_id == instrument_id)
        cyassert(strcmp(tc.v2_ticker, v2_ticker) == 0)

        if error_code == 0:
            tc.qmsg.instrument_index = instrument_index
            tc.qmsg.header.client_life_id = self.protocol.client_life_id
            tc.qmsg.header.server_life_id = self.protocol.server_life_id
            self.on_register_n_ok += 1
        else:
            printf(b'source_on_register_instrument: error %s -> rc %d', v2_ticker, error_code)
            self.on_register_n_err += 1

        if self.on_register_n_ok + self.on_register_n_err == self.hm_tickers.count():
            # All were registered
            return self.protocol.send_activate()
        return 1

    cdef int main(self) nogil:
        cdef void * transport_data
        cdef size_t msg_size = 0
        cdef int rc = 0
        cdef long dt_prev_call = datetime_nsnow()
        cdef long dt_now = datetime_nsnow()

        global global_is_shutting_down

        signal(SIGINT, sig_handler)
        signal(SIGQUIT, sig_handler)

        while not self.is_shutting_down and not global_is_shutting_down:
            zmq_poll(self.zmq_poll_array, 1, self.zmq_poll_timeout)

            if self.zmq_poll_array[0].revents & ZMQ_POLLIN:
                transport_data = self.transport_dealer.receive(&msg_size)
                if transport_data != NULL:

                    rc = self.protocol.on_process_new_message(transport_data, msg_size)
                    if rc < 0:
                        printf('protocol_source.on_process_new_message error: %d\n', rc)

                    self.transport_dealer.receive_finalize(transport_data)
                else:
                    printf('protocol_source.on_process_new_message transport_data is NULL\n')

            else:
                dt_now = datetime_nsnow()

            if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:

                # Avoid heart beating too frequently, but heartbeat intervals in seconds are managed by protocols
                rc = self.protocol.heartbeat(dt_now)
                if rc < 0:
                    printf('protocol_source.heartbeat error: %d\n', rc)

                dt_prev_call = dt_now

        if self.is_shutting_down:
            printf(b'UHFeed shutting down\n')
        elif global_is_shutting_down:
            printf(b'UHFeed shutting down by signal\n')

        #
        # Exit and finalize
        #
        self.transport_dealer.close()

        printf('DataSource stats:\n')
        printf('\tInstruments registered: OK: %d, Errs: %d\n', self.on_register_n_ok, self.on_register_n_err)
        printf('\tQuotes sent: %d\n', self.quotes_sent)
        printf('\tQuotes errors: %d\n', self.quotes_sent_errors)

