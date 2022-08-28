from libc.stdint cimport uint64_t
from uberhf.includes.uhfprotocols cimport MODULE_ID_UHFEED, ProtocolStatus
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSource
from uberhf.prototols.protocol_datafeed cimport ProtocolDataFeed
from uberhf.includes.utils cimport gen_lifetime_id
from .quotes_cache cimport SharedQuotesCache, QCRecord
from libc.stdio cimport printf


cdef class UHFeed(UHFeedAbstract):
    def __cinit__(self):
        self.uhfeed_life_id = gen_lifetime_id(MODULE_ID_UHFEED)
        self.quote_cache = SharedQuotesCache(self.uhfeed_life_id, 5, 10000)
        self.quotes_received = 0
        self.quotes_emitted = 0
        self.source_errors = 0
        self.quotes_errors = 0
        self.feed_errors = 0

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
            printf(b'source_on_initialize: success `%s` life_id: %ud\n', source_id, source_life_id)
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

    cdef int feed_on_subscribe(self, char * v2_ticker, unsigned int client_life_id, bint is_subscribe) nogil:
        cdef int rc = self.quote_cache.feed_on_subscribe(v2_ticker, <uint64_t>client_life_id, is_subscribe)
        if rc < 0:
            # TODO: log
            self.feed_errors += 1
            printf(b'feed_on_subscribe: error %d\n', rc)
        else:
            printf(b'feed_on_subscribe: success %s ClId: %ud\n', v2_ticker, client_life_id)

        return rc
