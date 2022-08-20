# cython: language_level=3
# distutils: sources = uberhf/includes/hashmapsrc.c uberhf/includes/safestr.c
cimport cython

from libc.stdlib cimport malloc, free
from libc.math cimport NAN, HUGE_VAL
from libc.limits cimport LONG_MAX
from libc.signal cimport raise_, SIGTRAP
from libc.stdint cimport uint64_t, uint16_t

from uberhf.includes.safestr cimport strlcpy
from uberhf.includes.asserts cimport cyassert, cybreakpoint


DEF TICKER_LEN = 30
DEF CRC_BEGIN = 64192
DEF CRC_END = 29517

cdef class HashMapMemPool(HashMapBase):
    @staticmethod
    cdef int _compare(const void *a, const void *b, void *udata) nogil:
        cdef TickerIdx *ta = <TickerIdx*>a
        cdef TickerIdx *tb = <TickerIdx*>b
        return strcmp(ta[0].ticker, tb[0].ticker)

    @staticmethod
    cdef uint64_t _hash(const void *item, uint64_t seed0, uint64_t seed1) nogil:
        cdef TickerIdx *t = <TickerIdx*>item
        return HashMapBase.hash_func(t[0].ticker, strlen(t[0].ticker), seed0, seed1)

    def __cinit__(self):
        self._new(sizeof(TickerIdx), self._hash, self._compare, 16)


@cython.final
cdef class MemPoolQuotes:
    """
    Memory based recent quotes cache
    """


    def __cinit__(self, int pool_capacity, long magic_number, shared_mem_file = None):
        """
        Initializing low level Cython stuff, this method is called with the same args as __init__

        :param pool_capacity:
        :param magic_number:
        :param shared_mem_file:
        :return:
        """
        # TODO: add pool capacity checks
        #print(sizeof(hashmap))
        self.pool_map = HashMapMemPool()
        self.pool_capacity = pool_capacity
        self.pool_cnt = 0
        self.n_errors = 0
        self.magic_number = magic_number
        self.shared_mem_file = shared_mem_file


        if self.shared_mem_file is None:
            # Use a simple malloc
            assert self.pool_capacity > 0
            self.pool_buffer = <void *> malloc(sizeof( QPoolHeader) + sizeof(QRec) * self.pool_capacity)
        else:
            # TODO: implement shared file here
            assert False

        self.header = (<QPoolHeader *> self.pool_buffer)
        self.header.magic_number = self.magic_number
        self.header.count = 0
        self.header.capacity = self.pool_capacity
        self.header.last_quote_utc = 0
        self.header.last_upd_utc = 0
        self.header.n_errors = 0

        self.quotes = <QRec*> (self.pool_buffer + sizeof(QPoolHeader))

        cdef QRec q
        cyassert(sizeof(q.ticker) == TICKER_LEN)





    cdef QRec* quote_get(self, char *ticker):
        cdef TickerIdx t
        cdef size_t len_copied = strlcpy(t.ticker, ticker, TICKER_LEN)
        if len_copied >= TICKER_LEN:
            # Ticker name overflow, just no updates!
            return NULL

        cdef TickerIdx * p_idx = <TickerIdx *> self.pool_map.get(&t)
        if p_idx == NULL:
            return NULL
        else:
            return &self.quotes[p_idx.idx_position]

    cdef bint quote_reset(self, char *ticker, QRec *q) nogil:
        """
        Fills empty and valid QRec, by reference, edit q in place! All `q` values are HUGE_VAL, i.e.
        will be ignored by quote_update(), you have to set only changed values intended for updates.
        
        :param ticker: ticker string (must be less than   TICKER_LEN constant, typically 29 symbols)
        :param q: pointer to QRec
        :return: 
            1 - if everything went fine
            0 - if q == NULL, buffer overflow, zero ticker length
        """
        if q == NULL:
            return 0

        cdef size_t len_copied = strlcpy(q.ticker, ticker, TICKER_LEN)
        if len_copied >= TICKER_LEN or len_copied == 0:
            # Ticker buffer overflow or empty string, reset ticker just in case
            if len_copied > 0:
                strlcpy(q.ticker, "", TICKER_LEN)
            q.crc_b = 0
            q.crc_e = 0
            return 0

        q.crc_b = CRC_BEGIN
        q.crc_e = CRC_END

        # These HUGE_VAL stuf, are the markers for the ignoring when quote update it partial
        #    incorrect values must set to NAN or zero (based on convention probably)
        q.bid = HUGE_VAL
        q.ask = HUGE_VAL
        q.last = HUGE_VAL
        q.bid_size = HUGE_VAL
        q.ask_size = HUGE_VAL
        q.last_upd_utc = LONG_MAX

        return 1

    cdef int quote_update(self, QRec * q) nogil:
        """
        Updates quotes pool / upserts new data if q.ticker doesn't exist in pool.
        
        Data will be copied from *q to self.quotes[idx]
        
        :param q: Quote data
                 
        :return: 
            - if OK: quote_pool_index >= 0, i.e. allowed to do self.quotes[quote_pool_index]
            - on Error negative value, error counter will be increased
            
            Error codes:
                -1 = q is NULL
                -2 = ticker length overflow
                -3 = pool capacity overflow
                -4 = zero ticker length
                -5 = q CRC magic number errors
                 
        """
        if q == NULL:
            # Null reference
            self.n_errors += 1
            self.header.n_errors += 1
            return -1
        if q.crc_b != CRC_BEGIN or q.crc_e != CRC_END:
            # Malformed quote
            self.n_errors += 1
            self.header.n_errors += 1
            return -5

        cdef int result = -10000
        cdef TickerIdx t
        cdef QRec def_q
        cdef QRec *pbuf_q
        cdef size_t len_copied
        #
        # C-struct trick here. QRec and TickerIdx both have `char ticker[TICKER_LEN]`
        #   as first element, which is used for hashing and ticker name comparison!
        cdef TickerIdx * p_idx = <TickerIdx*> self.pool_map.get(q)

        if p_idx == NULL:
            if self.pool_cnt == self.pool_capacity:
                # Quote Pool overflow
                self.n_errors += 1
                self.header.n_errors += 1
                return -3

            # Not exists adding one
            len_copied = strlcpy(t.ticker, q.ticker, TICKER_LEN)
            if len_copied >= TICKER_LEN:
                # Ticker name overflow, just no updates!
                self.n_errors += 1
                self.header.n_errors += 1
                return -2
            if len_copied == 0:
                # Ticker name empty
                self.n_errors += 1
                self.header.n_errors += 1
                return -4

            t.idx_position = self.header.count
            self.header.count += 1
            self.pool_cnt += 1
            self.pool_map.set(&t)

            self.quote_reset(q.ticker, &def_q)
            # Setting default but invalid values
            def_q.bid = NAN
            def_q.ask = NAN
            def_q.last = NAN
            def_q.bid_size = NAN
            def_q.ask_size = NAN
            def_q.last_upd_utc = 0

            self.quotes[t.idx_position] = def_q

            result = t.idx_position
            pbuf_q = self.quotes + t.idx_position
        else:
            result = p_idx.idx_position
            pbuf_q = self.quotes + p_idx.idx_position

        if q.ask != HUGE_VAL:
            pbuf_q.ask = q.ask

        if q.bid != HUGE_VAL:
            pbuf_q.bid = q.bid

        if q.last != HUGE_VAL:
            pbuf_q.last = q.last

        if q.ask_size != HUGE_VAL:
            pbuf_q.ask_size = q.ask_size

        if q.bid_size != HUGE_VAL:
            pbuf_q.bid_size = q.bid_size

        if q.last_upd_utc != LONG_MAX:
            pbuf_q.last_upd_utc = q.last_upd_utc


        return result


    def __dealloc__(self):
        if self.shared_mem_file is None:
            free(self.pool_buffer)
        else:
            # TODO: implement shared file deallocation
            assert False

        self.header = NULL
        self.quotes = NULL
        self.pool_buffer = NULL

