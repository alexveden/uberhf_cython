import zmq
from libc.string cimport strlen, strcpy, strcat, strcmp, strtok
from libc.stdio cimport printf
from libc.stdlib cimport malloc, free
from libc.string cimport strerror
from libc.errno cimport errno, ENOENT
from posix.unistd cimport sleep
from threading import Thread
#

# TODO: Make a benchmark!
#
from libc.signal cimport raise_, SIGTRAP
from uberhf.datafeed.mem_pool_quotes cimport MemPoolQuotes, QRec
from libc.stdlib cimport malloc, free, rand, srand, RAND_MAX
from libc.stdio cimport printf, sprintf
import time
from math import nan
import numpy as np
cimport numpy as np

from zmq cimport libzmq

DEF N_QUOTES = 1000000


cdef extern from "pthread.h":
    ctypedef unsigned  long int pthread_t;
    int pthread_detach(pthread_t thread)
    # pthread_create(... , void* attr, ...) - has to be a pthread_attr_t, but this type is ambiguous union
    #    which depends on OS type (maybe it's worth to investigate if needed)
    int pthread_create(pthread_t *thread,  void *attr, void *(*start_routine)(void *), void *arg)
    int pthread_join(pthread_t thread, void **retval);

cdef char * make_ticker(letter_arr):
    cdef char * buf = <char*>malloc(sizeof(char) * len(letter_arr) + 1)

    for i in range(len(letter_arr)):
        #raise_(SIGTRAP)
        buf[i] = <char>letter_arr[i]

    buf[len(letter_arr)] = b'\0'
    return buf

cdef void * thread_quote_processor_zmq(void * zmq_ctx) nogil:
    zmq_socket = libzmq.zmq_socket(zmq_ctx, libzmq.ZMQ_SUB)
    if zmq_socket == NULL:
        printf('NULL socket\n')
        return NULL

    zmq_result = libzmq.zmq_connect(zmq_socket, 'inproc://zmq-test')
    if zmq_result == 0:
        printf('Connection succeded\n')
    else:
        printf('Connection ERROR! %s\n', libzmq.zmq_strerror(libzmq.zmq_errno()))
        return NULL

    libzmq.zmq_setsockopt(zmq_socket, libzmq.ZMQ_SUBSCRIBE, b"", 0)

    #cdef char buffer[255]

    #with gil:
         # = MemPoolQuotes(N_QUOTES, 777111222)

    cdef QRec q;

    while True:
        data = libzmq.zmq_recvbuf(zmq_socket, &q, sizeof(QRec), 0)
        #c.quote_update(&q)

    return NULL



cdef void * thread_quote_processor_zmq(void * zmq_ctx) nogil:
    zmq_socket = libzmq.zmq_socket(zmq_ctx, libzmq.ZMQ_SUB)
    if zmq_socket == NULL:
        printf('NULL socket\n')
        return NULL

    zmq_result = libzmq.zmq_connect(zmq_socket, 'inproc://zmq-test')
    if zmq_result == 0:
        printf('Connection succeded\n')
    else:
        printf('Connection ERROR! %s\n', libzmq.zmq_strerror(libzmq.zmq_errno()))
        return NULL

    libzmq.zmq_setsockopt(zmq_socket, libzmq.ZMQ_SUBSCRIBE, b"", 0)

    #cdef char buffer[255]

    #with gil:
         # = MemPoolQuotes(N_QUOTES, 777111222)

    cdef QRec q;

    while True:
        data = libzmq.zmq_recvbuf(zmq_socket, &q, sizeof(QRec), 0)
        #c.quote_update(&q)

    return NULL

cpdef main():
    cdef int n_unique_tickers = 10000


    cdef char * ticker;
    cdef char ** all_tickers = <char**>malloc(sizeof(int*) * n_unique_tickers)
    cdef QRec* quotes = <QRec *>malloc(sizeof(QRec) * N_QUOTES)
    cdef int i;

    #np.random.random_integers(0, n_unique_tickers)
    ticker_arr = np.random.choice(np.array([l for l in 'ABCDEFGHJKLMNOPQRSTWXYZ'.encode()]), size=(n_unique_tickers, 10))

    for i, s in enumerate(ticker_arr):
        ticker = make_ticker(ticker_arr[i])
        all_tickers[i] = ticker
        #print(f"ticker: {ticker.decode('UTF-8')}", )


    cdef QRec q
    cdef int [:] rnd_ticker = np.random.randint(0, n_unique_tickers, size=N_QUOTES, dtype=np.int32)

    print('Beginning benchmark... C-fast')
    print(f'Unique tickers: {n_unique_tickers}')
    print(f'Quotes to process: {N_QUOTES}')
    t_begin = time.time()
    c = MemPoolQuotes(N_QUOTES, 777111222)

    for i in range(N_QUOTES):
        c.quote_reset(all_tickers[rnd_ticker[i]], &q)
        q.ask = i
        q.bid = i
        q.last_upd_utc = i
        q.ask_size = i
        q.bid_size = i
        c.quote_update(&q)

    t_end = time.time()
    print(f'Processed in {t_end-t_begin}sec, {N_QUOTES/(t_end-t_begin):0.0f} quotes/sec')
    print(f'Quote Pool count: {c.pool_cnt}')
    print(f'Quote Pool errors: {c.n_errors}')


    #
    # ZMQ + Threads
    #
    cdef void * zmq_ctx
    cdef void * zmq_socket
    cdef int zmq_result

    zmq_ctx = libzmq.zmq_ctx_new()
    assert zmq_ctx != NULL, zmq.strerror(zmq.zmq_errno())
    zmq_socket = libzmq.zmq_socket(zmq_ctx, libzmq.ZMQ_PUB)
    assert zmq_socket != NULL, zmq.strerror(zmq.zmq_errno())

    #cdef void * c_sock = zmq_socket.handle

    zmq_result = libzmq.zmq_bind(zmq_socket, 'inproc://zmq-test')
    assert zmq_result >= 0, zmq.strerror(zmq.zmq_errno())

    cdef char buff[255]

    cdef pthread_t thread;
    cdef retval = pthread_create(&thread, NULL, &thread_quote_processor_zmq, zmq_ctx)
    if retval != 0:
        printf('pthread_create error: %s\n', strerror(errno))
        exit(1)

    #t1 = Thread(target=countdown, args=(COUNT / 2,))

    #time.sleep(2)

    # for i in range(10):
    #     s = f'from_pub: {i}'.encode()
    #     #
    #     #    int zmq_sendbuf (void *s, const void *buf, size_t n, int flags)
    #
    #     print('Sending to ZMQ')
    #     libzmq.zmq_sendbuf(zmq_socket, b"test", 4, libzmq.ZMQ_SNDMORE)
    #     libzmq.zmq_sendbuf(zmq_socket, b'abs', 3, 0)
    #     time.sleep(1)

    print('Beginning benchmark... ZeroMQ inproc sockets')
    print(f'Unique tickers: {n_unique_tickers}')
    print(f'Quotes to process: {N_QUOTES}')
    t_begin = time.time()


    for i in range(N_QUOTES):
        c.quote_reset(all_tickers[rnd_ticker[i]], &q)
        q.ask = i
        q.bid = i
        q.last_upd_utc = i
        q.ask_size = i
        q.bid_size = i

        libzmq.zmq_sendbuf(zmq_socket, b"test", 4, libzmq.ZMQ_SNDMORE)
        libzmq.zmq_sendbuf(zmq_socket, &q, sizeof(QRec), 0)

    t_end = time.time()
    print(f'Processed in {t_end - t_begin}sec, {N_QUOTES / (t_end - t_begin):0.0f} quotes/sec')
    print(f'Quote Pool count: {c.pool_cnt}')
    print(f'Quote Pool errors: {c.n_errors}')

    pthread_join(thread, NULL)

    #
    # Mem clean
    #
    for i in range(n_unique_tickers):
        free(all_tickers[i])
    free(all_tickers)
    free(quotes)


