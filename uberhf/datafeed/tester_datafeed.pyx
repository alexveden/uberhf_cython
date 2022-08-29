from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.abstract_feedclient cimport FeedClientAbstract
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from .quotes_cache cimport SharedQuotesCache, QCRecord
from libc.stdint cimport uint64_t
from libc.stdio cimport printf
from uberhf.includes.utils cimport gen_lifetime_id, datetime_nsnow, TIMEDELTA_MILLI, timedelta_ns, strlcpy, random_int, TIMEDELTA_SEC, sleep_ns


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


cdef class DataFeedTester(FeedClientAbstract):
    def __cinit__(self, zmq_context_ptr, zmq_url_dealer, zmq_url_sub):
        self.transport_dealer = None
        self.transport_quote_sub = None
        self.qcache = None

        self.transport_dealer = Transport(<uint64_t> zmq_context_ptr, zmq_url_dealer, ZMQ_DEALER, b'DFEDT', router_id=b'UFEED')
        self.transport_quote_sub = Transport(<uint64_t> zmq_context_ptr, zmq_url_sub, ZMQ_SUB, b'DFEDT')

        self.protocol = ProtocolDataFeed(MODULE_ID_TEST_FEED, self.transport_dealer, self.transport_quote_sub, self, None)

        self.qcache = SharedQuotesCache(0, 0, 0)
        self.zmq_poll_timeout = 50
        self.zmq_poll_array[0] = [self.transport_quote_sub.socket, 0, ZMQ_POLLIN, 0]
        self.zmq_poll_array[1] = [self.transport_dealer.socket, 0, ZMQ_POLLIN, 0]
        self.is_shutting_down = 0

        self.n_src_status = 0
        self.n_quotes = 0
        self.n_subscriptions_confirmations = 0
        self.n_unsubscriptions_confirmations = 0
        self.n_instrument_info = 0
        self.n_subscribe_sent = 0
        self.n_subscribe_errors = 0

    cdef void close(self):
        if self.transport_quote_sub is not None:
            self.transport_quote_sub.close()
            self.transport_quote_sub = None
        if self.transport_dealer is not None:
            self.transport_dealer.close()
            self.transport_dealer = None
        if self.qcache is not None:
            self.qcache.close()
            self.qcache = None

    def __dealloc__(self):
        self.close()

    cdef void register_datafeed_protocol(self, object protocol):
        pass

    # Server confirms subscription / unsubscription
    cdef void feed_on_subscribe_confirm(self, char * v2_ticker, int instrument_index, bint is_subscribe) nogil:
        printf(b'feed_on_subscribe_confirm: %s -> idx: %d is_subscribe=%d\n', v2_ticker, instrument_index, is_subscribe)
        if is_subscribe:
            self.n_subscriptions_confirmations += 1
        else:
            self.n_unsubscriptions_confirmations += 1


    # Server reports the datasource status has changed
    cdef void feed_on_source_status(self, char * data_source_id, ProtocolStatus quotes_status) nogil:
        printf(b'feed_on_source_status: %s -> %d\n', data_source_id, quotes_status)
        self.n_src_status += 1

        cdef QCRecord * qr

        if quotes_status == ProtocolStatus.UHF_ACTIVE:
            # Let's subscribe all instruments
            for i in range(self.qcache.header.quote_count):
                qr = &self.qcache.records[i]
                printf(b'feed_on_source_status: Subscribing for quotes %s idx: %d\n', qr.v2_ticker, i)
                if self.protocol.send_subscribe(qr.v2_ticker) > 0:
                    self.n_subscribe_sent += 1
                else:
                    self.n_subscribe_errors += 1


    # Subscribed updates
    cdef void feed_on_quote(self, int instrument_index) nogil:
        #printf(b'feed_on_quote: %d\n', instrument_index)
        self.n_quotes += 1

    cdef void feed_on_instrumentinfo(self, int instrument_index) nogil:
        printf(b'feed_on_instrumentinfo: %d\n', instrument_index)
        self.n_instrument_info += 1

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
            zmq_poll(self.zmq_poll_array, 2, self.zmq_poll_timeout)

            if self.zmq_poll_array[0].revents & ZMQ_POLLIN:
                transport_data = self.transport_quote_sub.receive(&msg_size)
                if transport_data != NULL:
                    rc = self.protocol.on_process_new_message(transport_data, msg_size)
                    if rc < 0:
                        printf('transport_quote_sub.on_process_new_message error: %d\n', rc)

                    self.transport_quote_sub.receive_finalize(transport_data)
                else:
                    printf('transport_quote_sub.on_process_new_message transport_data is NULL\n')
            if self.zmq_poll_array[1].revents & ZMQ_POLLIN:
                transport_data = self.transport_dealer.receive(&msg_size)
                if transport_data != NULL:

                    rc = self.protocol.on_process_new_message(transport_data, msg_size)
                    if rc < 0:
                        printf('transport_dealer.on_process_new_message error: %d\n', rc)

                    self.transport_dealer.receive_finalize(transport_data)
                else:
                    printf('transport_dealer.on_process_new_message transport_data is NULL\n')
            else:
                dt_now = datetime_nsnow()

            if timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:

                # Avoid heart beating too frequently, but heartbeat intervals in seconds are managed by protocols
                rc = self.protocol.heartbeat(dt_now)
                if rc < 0:
                    printf('protocol_source.heartbeat error: %d\n', rc)

                dt_prev_call = dt_now

        if self.is_shutting_down:
            printf(b'DataFeedTester shutting down\n')
        elif global_is_shutting_down:
            printf(b'DataFeedTester shutting down by signal\n')

        #
        # Exit and finalize
        #
        self.transport_dealer.close()

        printf('DataFeedTester stats:\n')
        printf('\tInstruments subscriptions: +=%d, -=%d\n', self.n_subscriptions_confirmations, self.n_unsubscriptions_confirmations)
        printf('\tQuotes received: %d\n', self.n_quotes)
        printf('\tSource statuses received: %d\n', self.n_src_status)
        printf('\t# quotes in cache: %d\n', self.qcache.header.quote_count)
        printf('\t# sources in cache: %d\n', self.qcache.header.source_count)

    cdef void feed_on_initialize(self) nogil:
        printf(b'feed_on_initialize\n')

    cdef void feed_on_activate(self) nogil:
        printf(b'feed_on_activate\n')

    cdef void feed_on_disconnect(self) nogil:
        printf(b'feed_on_disconnect\n')