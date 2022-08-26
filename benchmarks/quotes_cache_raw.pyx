from libc.signal cimport raise_, SIGTRAP
from uberhf.datafeed.quotes_cache cimport SharedQuotesCache
from libc.stdlib cimport malloc, free, rand, srand, RAND_MAX
from libc.stdio cimport printf
import time
from math import nan
import numpy as np
cimport numpy as np
from uberhf.datafeed.quotes_cache cimport SharedQuotesCache, QCRecord, QCSourceHeader
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage

cdef char * make_ticker(letter_arr):
    cdef char * buf = <char*>malloc(sizeof(char) * len(letter_arr) + 1)

    for i in range(len(letter_arr)):
        #raise_(SIGTRAP)
        buf[i] = <char>letter_arr[i]

    buf[len(letter_arr)] = b'\0'
    return buf



cpdef main():
    cdef int n_unique_tickers = 10000
    cdef int n_quotes = 1000000

    cdef char * ticker;
    cdef char ** all_tickers = <char**>malloc(sizeof(int*) * n_unique_tickers)
    cdef int i;

    #np.random.random_integers(0, n_unique_tickers)
    ticker_arr = np.random.choice(np.array([l for l in 'ABCDEFGHJKLMNOPQRSTWXYZ'.encode()]), size=(n_unique_tickers, 10))

    for i, s in enumerate(ticker_arr):
        ticker = make_ticker(ticker_arr[i])
        all_tickers[i] = ticker
        #print(f"ticker: {ticker.decode('UTF-8')}", )


    cdef int [:] rnd_ticker = np.random.randint(0, n_unique_tickers, size=n_quotes, dtype=np.int32)

    print('Beginning benchmark... C-fast')
    print(f'Unique tickers: {n_unique_tickers}')
    print(f'Quotes to process: {n_quotes}')


    cdef SharedQuotesCache qc = SharedQuotesCache(777, 5, n_unique_tickers)
    cdef InstrumentInfo iinfo
    iinfo.tick_size = 10
    iinfo.min_lot_size = 5
    iinfo.margin_req = 100
    iinfo.theo_price = 200
    iinfo.price_scale = 2
    iinfo.usd_point_value = 1

    assert qc.source_initialize(b'test', 888) == 0
    for i in range(n_unique_tickers):
        rc = qc.source_register_instrument(b'test', all_tickers[i] , i+1, iinfo)
        assert rc >= 0, rc
        assert rc == i, rc
    assert qc.source_activate(b'test') == 0
    cdef ProtocolDSQuoteMessage msg
    cdef int instrument_idx
    t_begin = time.time()
    for i in range(n_quotes):
        instrument_idx = rnd_ticker[i]

        msg.instrument_index = instrument_idx
        msg.instrument_id = instrument_idx + 1
        msg.is_snapshot = 1
        msg.header.client_life_id = 888
        msg.header.server_life_id = 777
        msg.quote.bid = 100
        msg.quote.ask = 200
        msg.quote.bid_size = 1
        msg.quote.ask_size = 2
        msg.quote.last = 150
        msg.quote.last_upd_utc = 9999

        assert qc.source_on_quote(&msg) == instrument_idx

    t_end = time.time()
    print(f'Processed in {t_end-t_begin}sec, {n_quotes/(t_end-t_begin):0.0f} quotes/sec')
    print(f'Quote Pool count: {qc.header.quote_count}')
    print(f'Quote Pool quotes processed: {qc.sources[0].quotes_processed}')
    print(f'Quote Pool quote errors: {qc.header.quote_errors}')
    print(f'Quote Pool source errors: {qc.header.source_errors}')


    #
    # Mem clean
    #
    for i in range(n_unique_tickers):
        free(all_tickers[i])
    free(all_tickers)

    print('Sleeping 30 sec')
    time.sleep(30)
