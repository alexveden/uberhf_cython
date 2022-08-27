import time
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memcmp
import unittest
import zmq
from libc.math cimport isnan, NAN, HUGE_VAL
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.asserts cimport cyassert
from uberhf.prototols.protocol_base cimport ProtocolBase,  ProtocolBaseMessage, ConnectionState
from uberhf.includes.utils cimport datetime_nsnow, sleep_ns, timedelta_ns, TIMEDELTA_SEC, timer_nsnow, TIMEDELTA_MILLI
from uberhf.datafeed.quotes_cache cimport SharedQuotesCache, QCRecord, QCSourceHeader
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage
from posix.mman cimport shm_unlink

class CyQuotesCacheTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        shm_unlink(b'/uhfeed_shared_cache')

    def test_init(self):
        qc = SharedQuotesCache(1234, 5, 100)
        assert qc.uhffeed_life_id == 1234
        assert qc.is_server == 1
        assert qc.mmap_data != NULL
        assert qc.mmap_size == SharedQuotesCache.calc_shmem_size(5, 100)
        assert qc.header.uhffeed_life_id == 1234
        assert qc.header.magic_number == TRANSPORT_HDR_MGC
        assert qc.header.quote_count == 0
        assert qc.header.quote_capacity == 100
        assert qc.header.source_count == 0
        assert qc.header.source_capacity == 5
        assert qc.header.quote_errors == 0
        assert qc.header.source_errors == 0

        assert qc.ticker_map.count() == 0
        assert qc.source_map.count() == 0


    def test_source_initialize(self):
        qc = SharedQuotesCache(1234, 5, 100)
        assert qc.source_map.count() == 0
        assert qc.source_initialize(NULL, 12345) == -1
        assert qc.source_initialize(b'', 12345) == -1
        assert qc.source_initialize(b'123456', 12345) == -1
        assert qc.source_initialize(b'12345', 0) == -2
        assert qc.header.source_errors == 4

        assert qc.source_initialize(b'12345', 12345) == 0
        assert qc.source_map.get(b'12345') != NULL
        assert qc.sources[0].data_source_life_id == 12345
        assert qc.sources[0].data_source_id == b'12345'
        assert qc.sources[0].quotes_status == ProtocolStatus.UHF_INITIALIZING
        assert qc.sources[0].instruments_registered == 0
        assert qc.sources[0].quotes_processed == 0
        assert qc.sources[0].iinfo_processed == 0
        assert qc.sources[0].last_quote_ns == 0
        assert qc.sources[0].quote_errors == 0
        assert qc.sources[0].source_errors == 0
        assert qc.sources[0].magic_number == TRANSPORT_HDR_MGC

        assert qc.source_initialize(b'12345', 5678) == 0
        assert qc.sources[0].data_source_life_id == 5678
        assert qc.sources[0].data_source_id == b'12345'

        assert qc.source_initialize(b'1', 1) == 1
        assert qc.source_initialize(b'2', 2) == 2
        assert qc.source_initialize(b'3', 3) == 3
        assert qc.source_initialize(b'4', 4) == 4

        # Capacity overflow
        assert qc.source_initialize(b'5', 1) == -3
        assert qc.header.source_errors == 5

        assert qc.sources[1].data_source_life_id == 1
        assert qc.sources[2].data_source_life_id == 2
        assert qc.sources[3].data_source_life_id == 3
        assert qc.sources[4].data_source_life_id == 4

        assert qc.source_map.count() == 5

    def test_source_register_instrument(self):
        qc = SharedQuotesCache(1234, 5, 3)

        assert qc.source_initialize(b'12345', 12345) == 0

        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0

        cdef QCRecord q = qc.records[0]
        assert q.v2_ticker == b'RU.F.RTS'
        assert q.data_source_id == b'12345'
        assert q.instrument_id == 123
        assert q.data_source_hidx == 0
        assert q.magic_number == TRANSPORT_HDR_MGC

        assert q.iinfo.tick_size == iinfo.tick_size
        assert q.iinfo.min_lot_size == iinfo.min_lot_size
        assert q.iinfo.margin_req == iinfo.margin_req
        assert q.iinfo.price_scale == iinfo.price_scale

        assert isnan(q.quote.bid)
        assert isnan(q.quote.ask)
        assert isnan(q.quote.last)
        assert isnan(q.quote.bid_size)
        assert isnan(q.quote.ask_size)
        assert q.quote.last_upd_utc == 0


        iinfo.tick_size = 100
        iinfo.min_lot_size = 50
        iinfo.margin_req = 1000
        iinfo.theo_price = 2000
        iinfo.price_scale = 20
        iinfo.usd_point_value = 1
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0

        q = qc.records[0]
        assert q.v2_ticker == b'RU.F.RTS'
        assert q.data_source_id == b'12345'
        assert q.instrument_id == 123
        assert q.data_source_hidx == 0
        assert q.magic_number == TRANSPORT_HDR_MGC

        assert q.iinfo.tick_size == 100
        assert q.iinfo.min_lot_size == iinfo.min_lot_size
        assert q.iinfo.margin_req == iinfo.margin_req
        assert q.iinfo.price_scale == iinfo.price_scale

        assert isnan(q.quote.bid)
        assert isnan(q.quote.ask)
        assert isnan(q.quote.last)
        assert isnan(q.quote.bid_size)
        assert isnan(q.quote.ask_size)
        assert q.quote.last_upd_utc == 0
        assert qc.header.quote_count == 1

        assert qc.header.quote_errors == 0


    def test_source_register_instrument_errors(self):
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        qc = SharedQuotesCache(1234, 5, 2)
        assert qc.source_register_instrument(NULL, b'RU.F.RTS', 123, iinfo) == -1
        assert qc.source_register_instrument(b'', b'RU.F.RTS', 123, iinfo) == -1
        assert qc.source_register_instrument(b'123456', b'RU.F.RTS', 123, iinfo) == -1

        assert qc.source_register_instrument(b'1234', NULL, 123, iinfo) == -2
        assert qc.source_register_instrument(b'1234', b'', 123, iinfo) == -2
        assert qc.source_register_instrument(b'1234', b'0123456789012345678901234567890123456789', 123, iinfo) == -2
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 0, iinfo) == -3
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == -4

        assert qc.header.source_errors == 8

        assert qc.source_initialize(b'12345', 12345) == 0
        assert qc.source_initialize(b'1234', 12345) == 1
        cdef QCSourceHeader * src_h = &qc.sources[0]
        src_h.quotes_status = ProtocolStatus.UHF_ACTIVE
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == -5

        src_h.quotes_status = ProtocolStatus.UHF_INITIALIZING
        src_h.data_source_id[0] = b'8'
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == -5
        src_h.data_source_id[0] = b'1'

        assert qc.header.source_errors == 10
        assert src_h.source_errors == 2


        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qc.source_register_instrument(b'12345', b'RU.F.Si', 1233, iinfo) == 1
        assert qc.source_register_instrument(b'12345', b'RU.F.SiH9', 1233, iinfo) == -6

        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 1234, iinfo) == -7
        assert src_h.source_errors == 3, int(src_h.source_errors)

        assert qc.source_register_instrument(b'1234', b'RU.F.RTS', 123, iinfo) == -8
        assert qc.header.source_errors == 13, qc.header.source_errors

        assert qc.sources[0].source_errors == 3
        assert qc.sources[1].source_errors == 1

        assert qc.header.quote_count == 2
        assert qc.header.quote_capacity == 2
        assert src_h.instruments_registered == 2


    def test_source_activate(self):
        qc = SharedQuotesCache(1234, 5, 3)
        assert qc.source_activate(NULL) == -1
        assert qc.source_activate(b'') == -1
        assert qc.source_activate(b'123456') == -1
        assert qc.source_activate(b'12345') == -2
        assert qc.header.source_errors == 4

        assert qc.source_initialize(b'12345', 12345) == 0
        assert qc.source_activate(b'12345') == -3
        assert qc.header.source_errors == 5
        assert qc.sources[0].source_errors == 1
        assert qc.sources[0].quotes_status == ProtocolStatus.UHF_INACTIVE


        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        assert qc.source_initialize(b'12345', 12345) == 0
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qc.source_activate(b'12345') == 0
        assert qc.sources[0].quotes_status == ProtocolStatus.UHF_ACTIVE

    def test_source_disconnect(self):
        qc = SharedQuotesCache(1234, 5, 3)
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        assert qc.source_initialize(b'12345', 12345) == 0
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qc.source_activate(b'12345') == 0
        assert qc.sources[0].quotes_status == ProtocolStatus.UHF_ACTIVE
        assert qc.source_disconnect(b'12345') == 0
        assert qc.sources[0].quotes_status == ProtocolStatus.UHF_INACTIVE
        assert qc.sources[0].data_source_life_id == 0

        assert qc.source_disconnect(NULL) == -1
        assert qc.source_disconnect(b'') == -1
        assert qc.source_disconnect(b'123456') == -1
        assert qc.source_disconnect(b'777') == -2
        assert qc.header.source_errors == 4, qc.header.source_errors

    def test_source_on_quote(self):
        qc = SharedQuotesCache(777, 5, 3)
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        cdef ProtocolDSQuoteMessage msg
        msg.instrument_index = 0
        msg.instrument_id = 123
        msg.is_snapshot = 1
        msg.header.client_life_id = 888
        msg.header.server_life_id = 777
        msg.quote.bid = 100
        msg.quote.ask = 200
        msg.quote.bid_size = 1
        msg.quote.ask_size = 2
        msg.quote.last = 150
        msg.quote.last_upd_utc = 9999

        assert qc.source_initialize(b'12345', 888) == 0
        assert qc.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qc.source_activate(b'12345') == 0

        assert qc.source_on_quote(&msg) == 0
        assert qc.sources[0].quotes_processed == 1
        assert qc.sources[0].last_quote_ns == 9999

        cdef QCRecord * q = &qc.records[0]

        assert q.quote.bid == 100
        assert q.quote.ask == 200
        assert q.quote.bid_size == 1
        assert q.quote.ask_size == 2
        assert q.quote.last == 150
        assert q.quote.last_upd_utc == 9999

        # Next quote update partially
        msg.is_snapshot = 0
        msg.quote.bid = HUGE_VAL
        msg.quote.ask = HUGE_VAL
        msg.quote.bid_size = HUGE_VAL
        msg.quote.ask_size = HUGE_VAL
        msg.quote.last = HUGE_VAL
        msg.quote.last_upd_utc = 8888

        assert qc.source_on_quote(&msg) == 0
        q = &qc.records[0]

        assert qc.sources[0].quotes_processed == 2
        assert qc.sources[0].last_quote_ns == 8888

        assert q.quote.bid == 100
        assert q.quote.ask == 200
        assert q.quote.bid_size == 1
        assert q.quote.ask_size == 2
        assert q.quote.last == 150
        assert q.quote.last_upd_utc == 8888

        # Next quote update partially
        msg.is_snapshot = 0
        msg.quote.bid = 101
        msg.quote.ask = 201
        msg.quote.bid_size = 11
        msg.quote.ask_size = 12
        msg.quote.last = 151
        msg.quote.last_upd_utc = 8889

        assert qc.source_on_quote(&msg) == 0
        q = &qc.records[0]

        assert qc.sources[0].quotes_processed == 3
        assert qc.sources[0].last_quote_ns == 8889

        assert q.quote.bid == 101
        assert q.quote.ask == 201
        assert q.quote.bid_size == 11
        assert q.quote.ask_size == 12
        assert q.quote.last == 151
        assert q.quote.last_upd_utc == 8889

        msg.instrument_index = -1
        assert qc.source_on_quote(&msg) == -1
        assert qc.header.quote_errors == 1
        #assert qc.sources[0].quote_errors == 1

        msg.instrument_index = 1
        assert qc.source_on_quote(&msg) == -1
        assert qc.header.quote_errors == 2

        msg.instrument_index = 0
        msg.header.server_life_id = 98087798
        assert qc.source_on_quote(&msg) == -3
        assert qc.header.quote_errors == 3
        assert qc.sources[0].quote_errors == 1
        msg.header.server_life_id = 777


        msg.instrument_index = 0
        msg.header.client_life_id = 98087798
        assert qc.source_on_quote(&msg) == -2
        assert qc.header.quote_errors == 4
        assert qc.sources[0].quote_errors == 2
        msg.header.client_life_id = 888

        msg.instrument_id = 91828
        assert qc.source_on_quote(&msg) == -4
        assert qc.header.quote_errors == 5
        assert qc.sources[0].quote_errors == 3
        msg.instrument_id = 123


    def test_source_client_connect_memory_sharing(self):
        qs = SharedQuotesCache(1234, 5, 3)
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        qc = SharedQuotesCache(0, 0, 0)
        qc2 = SharedQuotesCache(0, 0, 0)
        qc3 = SharedQuotesCache(0, 0, 0)
        assert qc.mmap_data != qs.mmap_data
        assert qs.mmap_size == qc.mmap_size

        assert qs.source_initialize(b'12345', 12345) == 0
        assert qs.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qs.source_activate(b'12345') == 0

        assert qc.header.quote_count == 1
        assert qc.header.source_count == 1

        assert qc.ticker_map.count() == 0
        assert qc.source_map.count() == 0

        self.assertRaises(RuntimeError, SharedQuotesCache, 1234, 5, 3)

        assert memcmp(qs.mmap_data, qc.mmap_data, qs.mmap_size) == 0

        assert memcmp(&qs.sources[0], &qc.sources[0], sizeof(QCSourceHeader)) == 0
        assert qs.sources[0].magic_number == TRANSPORT_HDR_MGC

        assert memcmp(&qs.records[0], &qc.records[0], sizeof(QCRecord)) == 0
        assert qs.records[0].magic_number == TRANSPORT_HDR_MGC

    def test_source_client_get_quote(self):
        qs = SharedQuotesCache(1234, 5, 3)
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        qc = SharedQuotesCache(0, 0, 0)
        qc2 = SharedQuotesCache(0, 0, 0)
        qc3 = SharedQuotesCache(0, 0, 0)
        assert qc.mmap_data != qs.mmap_data
        assert qs.mmap_size == qc.mmap_size

        assert qs.source_initialize(b'12345', 888) == 0
        assert qs.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qs.source_activate(b'12345') == 0

        assert qc.get(NULL) == NULL
        assert qc.get(b'') == NULL
        assert qc.get(b'RU.F.SI') == NULL

        cdef QCRecord * qr = qc.get(b'RU.F.RTS')
        cdef QCSourceHeader * src_h = qc.get_source(b'12345')

        cdef ProtocolDSQuoteMessage msg
        msg.instrument_index = 0
        msg.instrument_id = 123
        msg.is_snapshot = 1
        msg.header.client_life_id = 888
        msg.header.server_life_id = 1234
        msg.quote.bid = 100
        msg.quote.ask = 200
        msg.quote.bid_size = 1
        msg.quote.ask_size = 2
        msg.quote.last = 150
        msg.quote.last_upd_utc = 9999
        assert qs.source_on_quote(&msg) == 0

        assert qr != NULL
        assert qr.v2_ticker == b'RU.F.RTS'
        assert qr.instrument_id == 123
        assert src_h.data_source_id == b'12345'
        assert src_h.instruments_registered == 1
        assert qr.quote.bid == 100

        #
        # Late intialization is also supported
        #
        assert qs.source_initialize(b'777', 12345) == 1
        assert qs.source_register_instrument(b'777', b'RU.F.Si', 9887, iinfo) == 1
        assert qs.source_activate(b'777') == 1

        cdef QCRecord * qr2 = qc.get(b'RU.F.Si')
        assert qr2 != NULL
        assert qr2.v2_ticker == b'RU.F.Si'
        assert qr2.instrument_id == 9887

        src_h = qc.get_source(b'777')
        assert src_h.data_source_id == b'777'
        assert src_h.instruments_registered == 1


        assert qc.get_source(NULL) == NULL
        assert qc.get_source(b'asdasd') == NULL

        # This will cause segmentation fault because the memory is readonly!
        #src_h.instruments_registered = 2

        msg.quote.bid = 101
        assert qs.source_on_quote(&msg) == 0

        # Bid also passed by reference
        assert qr.quote.bid == 101


    def test_source_server_early_close(self):
        qs = SharedQuotesCache(1234, 5, 3)
        cdef InstrumentInfo iinfo
        iinfo.tick_size = 10
        iinfo.min_lot_size = 5
        iinfo.margin_req = 100
        iinfo.theo_price = 200
        iinfo.price_scale = 2
        iinfo.usd_point_value = 1

        qc = SharedQuotesCache(0, 0, 0)
        assert qs.mmap_size == qc.mmap_size
        assert memcmp(qc.mmap_data, qs.mmap_data, qs.mmap_size) == 0

        assert qs.source_initialize(b'12345', 888) == 0
        assert qs.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qs.source_activate(b'12345') == 0

        cdef ProtocolDSQuoteMessage msg
        msg.instrument_index = 0
        msg.instrument_id = 123
        msg.is_snapshot = 1
        msg.header.client_life_id = 888
        msg.header.server_life_id = 1234
        msg.quote.bid = 100
        msg.quote.ask = 200
        msg.quote.bid_size = 1
        msg.quote.ask_size = 2
        msg.quote.last = 150
        msg.quote.last_upd_utc = 9999
        assert qs.source_on_quote(&msg) == 0
        cdef QCRecord * qr = qc.get(b'RU.F.RTS')
        assert qr.quote.bid == 100
        # server dies or closes
        qs.close()
        assert qs.mmap_data == NULL

        assert qc.get(NULL) == NULL
        assert qc.get(b'') == NULL
        assert qc.get(b'RU.F.SI') == NULL

        assert qc.get(b'RU.F.RTS') != NULL
        assert qc.get_source(b'12345') != NULL


        assert qc.get_source(b'12345').quotes_status == ProtocolStatus.UHF_INACTIVE
        assert qc.get_source(b'12345').data_source_life_id == 0
        # Server resets quotes after close!
        assert isnan(qr.quote.bid)

        #
        # New server restarted and running
        #
        qs2 = SharedQuotesCache(7907, 5, 3)
        assert qc.mmap_data != qs2.mmap_data
        assert qs2.mmap_size == qc.mmap_size
        assert qc.shmem_fd != qs2.shmem_fd
        assert memcmp(qc.mmap_data, qs2.mmap_data, qs2.mmap_size) == 0

        assert qc.header.source_count == 1
        assert qc.header.quote_count == 1
        assert qs2.header.source_count == 1
        assert qs2.header.quote_count == 1

        self.assertEqual(qs2.source_initialize(b'777', 12345), 1)
        assert qs2.source_register_instrument(b'777', b'RU.F.Si', 9887, iinfo) == 1
        assert qs2.source_activate(b'777') == 1

        assert qc.get(b'RU.F.RTS') != NULL
        assert qc.get(b'RU.F.Si') != NULL
        assert qc.get_source(b'777').quotes_status == ProtocolStatus.UHF_ACTIVE

        # Source was reset at the server restart routine
        assert qc.get_source(b'12345').quotes_status == ProtocolStatus.UHF_INACTIVE
        assert qc.get_source(b'12345').data_source_life_id == 0

        # Next initialization
        assert qs2.source_initialize(b'12345', 888) == 0
        assert qs2.source_register_instrument(b'12345', b'RU.F.RTS', 123, iinfo) == 0
        assert qs2.source_activate(b'12345') == 0

        assert qc.get_source(b'12345').quotes_status == ProtocolStatus.UHF_ACTIVE

        # Quotes were reset!
        assert isnan(qr.quote.bid)

    def test_client_early_connect(self):
        self.assertRaises(FileNotFoundError, SharedQuotesCache, 0, 0, 0)
        qs = SharedQuotesCache(1234, 5, 3)
        qc = SharedQuotesCache(0, 0, 0)



