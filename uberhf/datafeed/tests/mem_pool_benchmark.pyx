#
# TODO: Make a benchmark!
#
from libc.signal cimport raise_, SIGTRAP
from uberhf.datafeed.mem_pool_quotes cimport MemPoolQuotes, QRec
from libc.stdlib cimport malloc, free, rand, srand, RAND_MAX
from libc.stdio cimport printf
import time
from math import nan
import numpy as np
cimport numpy as np

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
    cdef QRec* quotes = <QRec *>malloc(sizeof(QRec) * n_quotes)
    cdef int i;

    #np.random.random_integers(0, n_unique_tickers)
    ticker_arr = np.random.choice(np.array([l for l in 'ABCDEFGHJKLMNOPQRSTWXYZ'.encode()]), size=(n_unique_tickers, 10))

    for i, s in enumerate(ticker_arr):
        ticker = make_ticker(ticker_arr[i])
        all_tickers[i] = ticker
        #print(f"ticker: {ticker.decode('UTF-8')}", )


    cdef QRec q
    cdef int [:] rnd_ticker = np.random.randint(0, n_unique_tickers, size=n_quotes, dtype=np.int32)

    print('Beginning benchmark... C-fast')
    print(f'Unique tickers: {n_unique_tickers}')
    print(f'Quotes to process: {n_quotes}')
    t_begin = time.time()
    c = MemPoolQuotes(n_quotes, 777111222)

    for i in range(n_quotes):
        c.quote_reset(all_tickers[rnd_ticker[i]], &q)
        q.ask = i
        q.bid = i
        q.last_upd_utc = i
        q.ask_size = i
        q.bid_size = i
        c.quote_update(&q)

    t_end = time.time()
    print(f'Processed in {t_end-t_begin}sec, {n_quotes/(t_end-t_begin):0.0f} quotes/sec')
    print(f'Quote Pool count: {c.pool_cnt}')
    print(f'Quote Pool errors: {c.n_errors}')

    print('Beginning benchmark... Python dict')
    print(f'Unique tickers: {n_unique_tickers}')
    print(f'Quotes to process: {n_quotes}')
    t_begin = time.time()

    quotes_map = {}
    for i in range(n_quotes):
        py_ticker = all_tickers[rnd_ticker[i]].decode('UTF-8')
        q_rec = quotes_map.get(py_ticker)
        if q_rec is None:
            q_rec = {'b': nan, 'a': nan, 'l': nan, 'bs': nan, 'as': nan, 'utc': 0}
            quotes_map[py_ticker] = q_rec

        q_rec['b'] = i
        q_rec['a'] = i
        q_rec['l'] = i
        q_rec['bs'] = i
        q_rec['as'] = i
        q_rec['utc'] = i


    t_end = time.time()
    print(f'Processed in {t_end - t_begin}sec, {n_quotes / (t_end - t_begin):0.0f} quotes/sec')
    print(f'Quote Pool count: {c.pool_cnt}')
    print(f'Quote Pool errors: {c.n_errors}')

    #
    # Mem clean
    #
    for i in range(n_unique_tickers):
        free(all_tickers[i])
    free(all_tickers)
    free(quotes)


