from libc.stdint cimport uint64_t
from uberhf.includes.uhfprotocols cimport MODULE_ID_UHFEED, ProtocolStatus
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSource
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from uberhf.prototols.transport cimport Transport
from uberhf.includes.utils cimport datetime_nsnow, TIMEDELTA_MILLI, timedelta_ns, sleep_ns
from .quotes_cache cimport SharedQuotesCache, QCRecord
from libc.stdio cimport printf
from uberhf.prototols.libzmq cimport *

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


cdef class UHFeed(UHFeedAbstract):
    def __cinit__(self, zmq_context_ptr, zmq_url_router, zmq_url_pub,  source_capacity=5, quote_capacity=10000):

        self.transport_router = None
        self.transport_pub = None
        self.quote_cache = None

        self.transport_router = Transport(<uint64_t>zmq_context_ptr, zmq_url_router, ZMQ_ROUTER, b'UFEED')
        self.transport_pub = Transport(<uint64_t>zmq_context_ptr, zmq_url_pub, ZMQ_PUB, b'UFEED')

        pfeed = ProtocolDataFeed(MODULE_ID_UHFEED, self.transport_router, self.transport_pub, None, self)
        psource = ProtocolDataSource(MODULE_ID_UHFEED, self.transport_router, None, self)

        self.uhfeed_life_id = psource.server_life_id
        self.quote_cache = SharedQuotesCache(self.uhfeed_life_id, source_capacity, quote_capacity)
        self.quotes_received = 0
        self.quotes_emitted = 0
        self.source_errors = 0
        self.quotes_errors = 0
        self.feed_errors = 0

        self.zmq_poll_timeout = 50
        self.zmq_poll_array[0] = [self.transport_router.socket, 0, ZMQ_POLLIN, 0]
        self.is_shutting_down = 0

    cdef void close(self):
        if self.transport_pub is not None:
            self.transport_pub.close()
            self.transport_pub = None
        if self.transport_router is not None:
            self.transport_router.close()
            self.transport_router = None
        if self.quote_cache is not None:
            self.quote_cache.close()
            self.quote_cache = None

    def __dealloc__(self):
        self.close()

    cdef void register_datasource_protocol(self, object protocol):
        self.protocol_source = <ProtocolDataSource> protocol

    cdef void register_datafeed_protocol(self, object protocol):
        self.protocol_feed = <ProtocolDataFeed> protocol


    cdef void source_on_initialize(self, char * source_id, unsigned int source_life_id) nogil:
        cdef int rc = self.quote_cache.source_initialize(source_id, source_life_id)
        if rc < 0:
            printf(b'source_on_initialize: error %d\n', rc)
            # TODO: log error
            self.source_errors += 1
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_ERROR)
        else:
            printf(b'source_on_initialize: success `%s` life_id: %u\n', source_id, source_life_id)
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_INITIALIZING)

    cdef void source_on_activate(self, char * source_id) nogil:
        cdef int rc = self.quote_cache.source_activate(source_id)
        if rc < 0:
            # TODO: log
            printf(b'source_on_activate: error %d\n', rc)
            self.source_errors += 1
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_ERROR)
        else:
            printf(b'source_on_activate: success %s\n', source_id)
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_ACTIVE)

    cdef void source_on_disconnect(self, char * source_id) nogil:
        cdef int rc = self.quote_cache.source_disconnect(source_id)
        if rc < 0:
            # TODO: log
            printf(b'source_on_disconnect: error %d\n', rc)
            self.source_errors += 1
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_ERROR)
        else:
            printf(b'source_on_disconnect: success %s\n', source_id)
            self.protocol_feed.send_source_status(source_id, ProtocolStatus.UHF_INACTIVE)

    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfo * iinfo) nogil:
        cdef int rc = self.quote_cache.source_register_instrument(source_id, v2_ticker, instrument_id, iinfo)
        if rc < 0:
            # TODO: log
            printf(b'source_on_register_instrument: error %d\n', rc)
            self.source_errors += 1
        else:
            printf(b'source_on_register_instrument: success %s: %s\n', source_id, v2_ticker)
        return rc

    cdef void source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil:
        cdef QCRecord * qr
        cdef int result_idx = self.quote_cache.source_on_quote(msg)
        if result_idx < 0:
            # TODO: log
            self.quotes_errors += 1
            printf(b'source_on_quote: error %d\n', result_idx)
        else:
            self.quotes_received += 1
            qr = &self.quote_cache.records[result_idx]

            if qr.subscriptions_bits != 0:
                if self.protocol_feed.send_feed_update(result_idx, 1, qr.subscriptions_bits) > 0:
                    self.quotes_emitted += 1
                    # Success
                    pass
                else:
                    self.feed_errors += 1
                    # Error
                    pass

    cdef void feed_on_activate(self, char * feed_id) nogil:
        """
        Feed completed initialization and ready to activate
        
        When new source connected, the UHFeed will send the status of all datasources initially
        :return: 
        """
        printf(b'feed_on_activate: %s\n', feed_id)
        for i in range(self.quote_cache.header.source_count):
            self.protocol_feed.send_source_status(self.quote_cache.sources[i].data_source_id, self.quote_cache.sources[i].quotes_status)

    cdef int feed_on_subscribe(self, char * v2_ticker, unsigned int client_life_id, bint is_subscribe) nogil:
        cdef int rc = self.quote_cache.feed_on_subscribe(v2_ticker, <uint64_t>client_life_id, is_subscribe)
        if rc < 0:
            # TODO: log
            self.feed_errors += 1
            printf(b'feed_on_subscribe: error %d\n', rc)
        else:
            printf(b'feed_on_subscribe: success %s ClId: %ud\n', v2_ticker, client_life_id)

        return rc

    cdef int main(self) nogil:
        cdef void * transport_data
        cdef size_t msg_size = 0
        cdef int rc = 0
        cdef long dt_prev_call = datetime_nsnow()
        cdef long dt_now = datetime_nsnow()
        cdef bint has_data = 0


        global global_is_shutting_down

        signal(SIGINT, sig_handler)
        signal(SIGQUIT, sig_handler)

        while not self.is_shutting_down and not global_is_shutting_down:
            zmq_poll(self.zmq_poll_array, 1, self.zmq_poll_timeout)

            if self.zmq_poll_array[0].revents & ZMQ_POLLIN:
                transport_data = self.transport_router.receive(&msg_size)

                rc = self.protocol_source.on_process_new_message(transport_data, msg_size)
                if rc < 0:
                    printf('protocol_source.on_process_new_message error: %d\n', rc)
                elif rc == 0:
                    rc = self.protocol_feed.on_process_new_message(transport_data, msg_size)

                    if rc < 0:
                        printf('protocol_feed.on_process_new_message error: %d\n', rc)

                self.transport_router.receive_finalize(transport_data)
                has_data = 1
            else:
                dt_now = datetime_nsnow()
                has_data = 0

            if not has_data and timedelta_ns(dt_now, dt_prev_call, TIMEDELTA_MILLI) >= 50:

                # Avoid heart beating too frequently, but heartbeat intervals in seconds are managed by protocols
                rc = self.protocol_source.heartbeat(dt_now)
                if rc < 0:
                    printf('protocol_source.heartbeat error: %d\n', rc)

                rc = self.protocol_feed.heartbeat(dt_now)
                if rc < 0:
                    printf('protocol_feed.heartbeat error: %d\n', rc)

                dt_prev_call = dt_now

        if self.is_shutting_down:
            printf(b'UHFeed shutting down\n')
        elif global_is_shutting_down:
            printf(b'UHFeed shutting down by signal\n')

        printf('UHFeed stats:\n')
        printf('\tSources registered: %d\n', self.quote_cache.header.source_count)
        printf('\tInstruments registered: %d\n', self.quote_cache.header.quote_count)
        printf('\tQuotes received: %d\n', self.quotes_received)
        printf('\tQuotes emitted: %d\n', self.quotes_emitted)
        printf('\tQuotes errors: %d\n', self.quotes_errors)


        #
        # Exit and finalize
        #
        self.transport_router.close()
        self.transport_pub.close()
