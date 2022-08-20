import time
import unittest
import zmq

from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport datetime_nsnow, TIMEDELTA_SEC, timedelta_ns, TIMEDELTA_MICRO
from posix.time cimport clock_getres, clock_gettime, timespec, CLOCK_REALTIME, CLOCK_REALTIME_COARSE, CLOCK_MONOTONIC_COARSE, CLOCK_MONOTONIC
from posix.time cimport timespec, nanosleep
from libc.limits cimport LONG_MAX
from libc.stdint cimport uint64_t
from uberhf.prototols.protocol_datasource import ProtocolDatasourceServer


import os
import pytest

URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'


class CyProtocolDatasourceTestCase(unittest.TestCase):

    def test_protocol_init_server(self):
        cdef timespec tim, tim2;
        tim.tv_sec = 1
        tim.tv_nsec = 0

        cdef long now_start = datetime_nsnow()
        nanosleep(&tim, &tim2)
        cdef long now = datetime_nsnow()

        print(now_start)
        print(now)
        print(now-now_start)
        print(<double>LONG_MAX/<double>now)

        print(f'clock resolutions')
        cdef timespec tr
        clock_getres(CLOCK_REALTIME, &tr)
        print(f'CLOCK_REALTIME resolution: {tr.tv_nsec}ns')
        clock_getres(CLOCK_REALTIME_COARSE, &tr)
        print(f'CLOCK_REALTIME_COARSE resolution: {tr.tv_nsec}ns')
        clock_getres(CLOCK_MONOTONIC_COARSE, &tr)
        print(f'CLOCK_MONOTONIC_COARSE resolution: {tr.tv_nsec}ns')

        cdef timespec spec
        clock_gettime(CLOCK_REALTIME, &spec)
        now_start = spec.tv_sec * 1000000000 + spec.tv_nsec
        # Exclude 1 to get end time
        N = 1000000
        for i in range(N-1):
            spec.tv_sec = 0
            spec.tv_nsec = 0
            clock_gettime(CLOCK_REALTIME, &spec)
        clock_gettime(CLOCK_REALTIME, &spec)
        now = spec.tv_sec * 1000000000 + spec.tv_nsec
        print(f'CLOCK_REALTIME: speed {(now-now_start)/N}ns per call')

        clock_gettime(CLOCK_MONOTONIC, &spec)
        now_start = spec.tv_sec * 1000000000 + spec.tv_nsec
        # Exclude 1 to get end time
        N = 1000000
        for i in range(N - 1):
            spec.tv_sec = 0
            spec.tv_nsec = 0
            clock_gettime(CLOCK_MONOTONIC, &spec)
        clock_gettime(CLOCK_MONOTONIC, &spec)
        now = spec.tv_sec * 1000000000 + spec.tv_nsec
        print(f'CLOCK_MONOTONIC: speed {(now - now_start) / N}ns per call')

        clock_gettime(CLOCK_MONOTONIC, &spec)
        now_start = spec.tv_sec * 1000000000 + spec.tv_nsec
        # Exclude 1 to get end time
        N = 1000000
        for i in range(N - 1):
            datetime_nsnow()
        clock_gettime(CLOCK_MONOTONIC, &spec)
        now = spec.tv_sec * 1000000000 + spec.tv_nsec
        print(f'datetime_nsnow: speed {(now - now_start) / N}ns per call')

        clock_gettime(CLOCK_REALTIME, &spec)
        now_start = spec.tv_sec * 1000000000 + spec.tv_nsec
        # Exclude 1 to get end time
        for i in range(N - 1):
            spec.tv_sec = 0
            spec.tv_nsec = 0
            clock_gettime(CLOCK_REALTIME_COARSE, &spec)
        clock_gettime(CLOCK_REALTIME, &spec)
        now = spec.tv_sec * 1000000000 + spec.tv_nsec
        print(f'CLOCK_REALTIME_COARSE: speed {(now - now_start) / N}ns per call')


        assert self.assertEqual(timedelta_ns(now, now_start, TIMEDELTA_MICRO), 1000)
        pass
        # cdef char buffer[255]
        # cdef size_t buffer_size
        #
        # with zmq.Context() as ctx:
        #     transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT , ZMQ_DEALER, b'CLI')
        #     transport_s = Transport(<uint64_t> ctx.underlying, URL_CONNECT, ZMQ_ROUTER, b'SRV')
        #     try:
        #
        #         pass
        #     finally:
        #         transport_s.close()
        #         transport_c.close()
