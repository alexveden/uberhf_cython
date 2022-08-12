# distutils: include_dirs = uberhf/include/
import unittest

# cdef-classes require cimport and .pxd file!
from libc.string cimport strcmp, strcpy
from libc.math cimport  HUGE_VAL, isnan
from libc.limits cimport LONG_MAX
from uberhf.datafeed.mem_pool_quotes import MemPoolQuotes as mpq
from uberhf.datafeed.mem_pool_quotes cimport MemPoolQuotes, QRec



class CythonTestCase(unittest.TestCase):
    def test_python(self):
        py_pool = mpq(5, 22277)

    # IMPORTANT: in some reason Nose test doesn't recognize this module as a test
    def test_mem_quotes_init(self):
        c = MemPoolQuotes(5, 777111222)
        self.assertEqual(5, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)
        self.assertEqual(777111222, c.magic_number)

    def test_quote_reset(self):
        c = MemPoolQuotes(5, 777111222)
        cdef QRec q;
        self.assertEqual(1, c.quote_reset('AAPL', &q))
        self.assertEqual(0, strcmp(q.ticker, "AAPL"))

        self.assertEqual(1, c.quote_reset('GAZP', &q))
        self.assertEqual(0, strcmp(q.ticker, "GAZP"))

        self.assertEqual(HUGE_VAL, q.ask)
        self.assertEqual(HUGE_VAL, q.bid)
        self.assertEqual(HUGE_VAL, q.last)
        self.assertEqual(HUGE_VAL, q.ask_size)
        self.assertEqual(HUGE_VAL, q.bid_size)
        self.assertEqual(LONG_MAX, q.last_upd_utc)
        self.assertEqual(64192, q.crc_b)
        self.assertEqual(29517, q.crc_e)

        # 29-len ticker
        self.assertEqual(1, c.quote_reset('01234567890123456789012345678', &q))
        self.assertEqual(0, strcmp(q.ticker, '01234567890123456789012345678'))

        # 30-len ticker - buffer overflow! return false
        self.assertEqual(0, c.quote_reset('012345678901234567890123456789', &q))
        # Also erase ticker value
        self.assertEqual(0, strcmp(q.ticker, ''))
        self.assertEqual(0, q.crc_b)
        self.assertEqual(0, q.crc_e)

    def test_mem_quotes_init_empty(self):
        c = MemPoolQuotes(5, 777111222)
        self.assertEqual(5, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        cdef QRec q;
        cdef QRec *pq;
        self.assertEqual(1, c.quote_reset('AAPL', &q))

        cdef int qidx = c.quote_update(&q)
        self.assertEqual(0, qidx)
        pq = c.quotes + qidx

        self.assertEqual(0, strcmp(pq.ticker, 'AAPL'))
        self.assertEqual(True, isnan(pq.bid))
        self.assertEqual(True, isnan(pq.ask))
        self.assertEqual(True, isnan(pq.last))
        self.assertEqual(True, isnan(pq.ask_size))
        self.assertEqual(True, isnan(pq.bid_size))
        self.assertEqual(0, pq.last_upd_utc)

    def test_quote_update_signle(self):
        c = MemPoolQuotes(5, 777111222)
        self.assertEqual(5, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        cdef QRec q_appl;
        c.quote_reset('AAPL', &q_appl)
        q_appl.ask = 1
        q_appl.bid = 2
        q_appl.last = 3
        q_appl.ask_size = 4
        q_appl.bid_size = 5
        q_appl.last_upd_utc = 6
        q_pos = c.quote_update(&q_appl)

        self.assertEqual(q_pos, 0)
        self.assertEqual(c.quotes[q_pos].ask, 1)
        self.assertEqual(c.quotes[q_pos].bid, 2)
        self.assertEqual(c.quotes[q_pos].last, 3)
        self.assertEqual(c.quotes[q_pos].ask_size, 4)
        self.assertEqual(c.quotes[q_pos].bid_size, 5)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 6)

        c.quote_reset('AAPL', &q_appl)
        q_appl.ask = 11
        q_appl.bid = 12
        q_appl.last = 13
        q_appl.ask_size = 14
        q_appl.bid_size = 15
        q_appl.last_upd_utc = 16
        q_pos = c.quote_update(&q_appl)

        self.assertEqual(q_pos, 0)
        self.assertEqual(c.quotes[q_pos].ask, 11)
        self.assertEqual(c.quotes[q_pos].bid, 12)
        self.assertEqual(c.quotes[q_pos].last, 13)
        self.assertEqual(c.quotes[q_pos].ask_size, 14)
        self.assertEqual(c.quotes[q_pos].bid_size, 15)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 16)

        # Reset quote, update only bid and ask
        c.quote_reset('AAPL', &q_appl)
        q_appl.ask = 21
        q_appl.bid = 22
        q_pos = c.quote_update(&q_appl)

        self.assertEqual(q_pos, 0)
        self.assertEqual(c.quotes[q_pos].ask, 21)
        self.assertEqual(c.quotes[q_pos].bid, 22)
        self.assertEqual(c.quotes[q_pos].last, 13)
        self.assertEqual(c.quotes[q_pos].ask_size, 14)
        self.assertEqual(c.quotes[q_pos].bid_size, 15)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 16)

    def test_quote_update_multiple(self):
        c = MemPoolQuotes(5, 777111222)
        self.assertEqual(5, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        cdef QRec q_appl;
        cdef QRec q_gazp;
        c.quote_reset('AAPL', &q_appl)
        q_appl.ask = 1
        q_appl.bid = 2
        q_appl.last = 3
        q_appl.ask_size = 4
        q_appl.bid_size = 5
        q_appl.last_upd_utc = 6
        q_pos = c.quote_update(&q_appl)

        self.assertEqual(q_pos, 0)
        self.assertEqual(0, strcmp(c.quotes[q_pos].ticker, 'AAPL'))
        self.assertEqual(c.quotes[q_pos].ask, 1)
        self.assertEqual(c.quotes[q_pos].bid, 2)
        self.assertEqual(c.quotes[q_pos].last, 3)
        self.assertEqual(c.quotes[q_pos].ask_size, 4)
        self.assertEqual(c.quotes[q_pos].bid_size, 5)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 6)

        c.quote_reset('GAZP', &q_gazp)
        q_gazp.ask = 11
        q_gazp.bid = 12
        q_gazp.last = 13
        q_gazp.ask_size = 14
        q_gazp.bid_size = 15
        q_gazp.last_upd_utc = 16
        q_pos = c.quote_update(&q_gazp)

        self.assertEqual(q_pos, 1)
        self.assertEqual(0, strcmp(c.quotes[q_pos].ticker, 'GAZP'))
        self.assertEqual(c.quotes[q_pos].ask, 11)
        self.assertEqual(c.quotes[q_pos].bid, 12)
        self.assertEqual(c.quotes[q_pos].last, 13)
        self.assertEqual(c.quotes[q_pos].ask_size, 14)
        self.assertEqual(c.quotes[q_pos].bid_size, 15)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 16)


        self.assertEqual(c.pool_cnt, 2)
        self.assertEqual(c.header.count, 2)
        self.assertEqual(c.n_errors, 0)
        self.assertEqual(c.header.n_errors, 0)

    def test_quote_update_make_sure_data_is_copied(self):
        c = MemPoolQuotes(5, 777111222)
        self.assertEqual(5, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        cdef QRec q_appl;
        c.quote_reset('AAPL', &q_appl)
        q_appl.ask = 1
        q_appl.bid = 2
        q_appl.last = 3
        q_appl.ask_size = 4
        q_appl.bid_size = 5
        q_appl.last_upd_utc = 6
        q_pos = c.quote_update(&q_appl)

        #
        # Change q_appl and make sure that quote buffer was not affected
        #
        q_appl.ask = 11
        q_appl.bid = 12
        q_appl.last = 13
        q_appl.ask_size = 14
        q_appl.bid_size = 15
        q_appl.last_upd_utc = 16

        self.assertEqual(q_pos, 0)
        self.assertEqual(0, strcmp(c.quotes[q_pos].ticker, 'AAPL'))
        self.assertEqual(c.quotes[q_pos].ask, 1)
        self.assertEqual(c.quotes[q_pos].bid, 2)
        self.assertEqual(c.quotes[q_pos].last, 3)
        self.assertEqual(c.quotes[q_pos].ask_size, 4)
        self.assertEqual(c.quotes[q_pos].bid_size, 5)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 6)

        # Replacing the buffer
        c.quote_reset('GAZP', &q_appl)

        self.assertEqual(q_pos, 0)
        self.assertEqual(0, strcmp(c.quotes[q_pos].ticker, 'AAPL'))
        self.assertEqual(c.quotes[q_pos].ask, 1)
        self.assertEqual(c.quotes[q_pos].bid, 2)
        self.assertEqual(c.quotes[q_pos].last, 3)
        self.assertEqual(c.quotes[q_pos].ask_size, 4)
        self.assertEqual(c.quotes[q_pos].bid_size, 5)
        self.assertEqual(c.quotes[q_pos].last_upd_utc, 6)

    def test_quote_update_errors_capacity_oveflow(self):
        c = MemPoolQuotes(2, 777111222)
        self.assertEqual(2, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        cdef QRec q;
        c.quote_reset('AAPL', &q)
        self.assertEqual(0, c.quote_update(&q))
        self.assertEqual(c.n_errors, 0)
        self.assertEqual(c.header.n_errors, 0)

        c.quote_reset('GAZP', &q)
        self.assertEqual(1, c.quote_update(&q))
        self.assertEqual(c.n_errors, 0)
        self.assertEqual(c.header.n_errors, 0)

        self.assertEqual(1, c.quote_reset('MSFT', &q))
        self.assertEqual(-3, c.quote_update(&q))
        self.assertEqual(c.n_errors, 1)
        self.assertEqual(c.header.n_errors, 1)

    def test_quote_update_errors__null_ref(self):
        c = MemPoolQuotes(2, 777111222)
        self.assertEqual(2, c.pool_capacity)
        self.assertEqual(0, c.pool_cnt)

        #
        # Null pointer = -1
        #
        self.assertEqual(-1, c.quote_update(NULL))

        #
        # Quote reset_null pointed
        #
        self.assertEqual(0, c.quote_reset('AAPL', NULL))

        #
        # Unset / malformed quote (crc check)
        #
        cdef QRec q;
        self.assertEqual(-5, c.quote_update(&q))

        #
        # Zero length ticker at c.quote_reset
        #
        self.assertEqual(0, c.quote_reset('', &q))
        self.assertEqual(-5, c.quote_update(&q))

        #
        # Zero length ticker later
        #
        self.assertEqual(1, c.quote_reset('AAPL', &q))
        strcpy(q.ticker, '')
        self.assertEqual(-4, c.quote_update(&q))

        self.assertEqual(c.n_errors, 4)
        self.assertEqual(c.header.n_errors, 4)