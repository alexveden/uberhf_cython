import time
import unittest
import zmq
import datetime
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport random_float, random_seed, random_int, sleep_ns, timer_nsnow, datetime_from_spec, gen_lifetime_id
from posix.time cimport clock_getres, clock_gettime, timespec, CLOCK_REALTIME, CLOCK_REALTIME_COARSE, CLOCK_MONOTONIC_COARSE, CLOCK_MONOTONIC
from posix.time cimport timespec, nanosleep

from uberhf.includes.utils cimport datetime_nsnow, TIMEDELTA_SEC, timedelta_ns, TIMEDELTA_MILLI
import pandas as pd


class CyUtilsTestCase(unittest.TestCase):

    def test_random(self):
        #random_seed(7770912)
        cdef double prev_rndom = -1
        cdef double r = 0
        cdef bint has_lo = 0
        cdef bint has_hi = 0

        for i in range(1000000):
            r = random_float()

            if i > 0:
                assert prev_rndom != r
                assert r >= 0
                assert r < 1
                if r == 0:
                    has_lo = 1
                if r == 1:
                    has_hi = 1

            prev_rndom = r

        assert has_hi == 0
        #assert has_lo == 1   # HM this seems never trigger! weird


    def test_random_seed(self):
        cdef double prev_rndom = -1
        cdef double r = 0

        for i in range(1000):
            random_seed(777)
            r = random_float()

            if i > 0:
                assert prev_rndom == r

            prev_rndom = r


    def test_random_int(self):
        random_seed(7770912)
        cdef double r = 0

        for i in range(100000):
            r = random_int(0, 100)
            assert r >= 0
            assert r < 100

        random_seed(941342)
        for i in range(100000):
            r = random_int(-100, 0)
            assert r >= -100
            assert r < 0
        random_seed(941342)
        for i in range(1000):
            r = random_int(-1, -1)
            assert r == -1

    def test_sleep(self):
        cdef timespec tim, tim2;
        tim.tv_sec = 1
        tim.tv_nsec = 0

        cdef long now_start = timer_nsnow()
        nanosleep(&tim, &tim2)
        cdef long now = timer_nsnow()
        self.assertAlmostEqual(timedelta_ns(now, now_start, TIMEDELTA_MILLI), 1000, delta=2)

        now_start = datetime_nsnow()
        sleep_ns(1.0)
        now = datetime_nsnow()
        self.assertAlmostEqual(timedelta_ns(now, now_start, TIMEDELTA_SEC), 1, delta=0.002)

        now_start = datetime_nsnow()
        sleep_ns(0.1)
        now = datetime_nsnow()
        self.assertAlmostEqual(timedelta_ns(now, now_start, TIMEDELTA_MILLI), 100, delta=2)

    def test_datetime_now(self):
        cdef long now = datetime_nsnow()
        cy_now_microsec = datetime.datetime.utcfromtimestamp(now/10**9).replace(microsecond=0)
        py_now =  datetime.datetime.utcnow().replace(microsecond=0)
        py_cython = pd.Timestamp(now).replace(microsecond=0, nanosecond=0)

        self.assertEqual(py_cython, py_now)
        self.assertEqual(cy_now_microsec, py_now)

    def test_datetimefrom_spec(self):
        cdef long now = datetime_nsnow()
        cdef timespec spec
        clock_gettime(CLOCK_REALTIME, &spec)
        cdef long spec_now = datetime_from_spec(&spec)

        assert spec_now > 0
        assert now > 0
        self.assertAlmostEqual(now, spec_now, delta=10000)

    def test_gen_lifetime_id(self):
        cdef long now = datetime_nsnow()
        dt = datetime.datetime.now()
        cdef unsigned int ltid = gen_lifetime_id(40)

        self.assertEqual(<unsigned int>(ltid/100), (100000000 * 40 + 1000000*dt.hour+ 10000 * dt.minute + 100 * dt.second)/100)
        self.assertGreater(<unsigned int> (ltid % 100), 0)




