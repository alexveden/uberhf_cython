from uberhf.orders.fix_orders import FIXNewOrderSingle, FIXNewOrderSinglePy
from uberhf.includes.utils cimport timer_nsnow, timedelta_ns, TIMEDELTA_SEC, random_int
from libc.stdio cimport sprintf


cpdef main():
    cdef int msg_size = 0
    cdef int n_steps = 1000000
    cdef long t_start
    cdef long t_end
    cdef double duration
    cdef int i = 0
    cdef bytes _clord
    cdef double px


    msg_size = 100
    print('-' * 100)
    print(f'FIX Binary MSG Based')
    print(f'FIXNewOrderSingle constructor')
    t_start = timer_nsnow()
    for i in range(n_steps):
        o = FIXNewOrderSingle(b'220908-102100-00001', b'acc', None, 100, -1)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSingle constructor: {n_steps / duration} msg/sec')



    print(f'FIXNewOrderSingle cancel req')
    o = FIXNewOrderSingle(b'123', b'acc', None, 100, -1)
    t_start = timer_nsnow()
    for i in range(n_steps):
        cxlreq = o.cancel_req(b'234')
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSingle cancel req: {n_steps / duration} msg/sec')


    print(f'FIXNewOrderSingle get clord_id')
    o = FIXNewOrderSingle(b'123', b'acc', None, 100, -1)
    t_start = timer_nsnow()
    for i in range(n_steps):
        _clord = o.clord_id
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSingle get clord_id: {n_steps / duration} msg/sec')


    print(f'FIXNewOrderSingle get price')
    o = FIXNewOrderSingle(b'123', b'acc', None, 100, -1)
    t_start = timer_nsnow()
    for i in range(n_steps):
        price = o.price
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSingle get price: {n_steps / duration} msg/sec')



    msg_size = 100
    print('-' * 100)
    print(f'PYTHON CLASS BASED')
    t_start = timer_nsnow()
    for i in range(n_steps):
        o = FIXNewOrderSinglePy(b'220908-102100-00001', b'acc', None, 100, -1)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSinglePY constructor: {n_steps / duration} msg/sec')

    o = FIXNewOrderSinglePy(b'123', b'acc', None, 100, -1)
    t_start = timer_nsnow()
    for i in range(n_steps):
        _clord = o.clord_id
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSinglePY get clord_id: {n_steps / duration} msg/sec')


    o = FIXNewOrderSinglePy(b'123', b'acc', None, 100, -1)
    t_start = timer_nsnow()
    for i in range(n_steps):
        price = o.px
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    assert duration != 0
    print(f'FIXNewOrderSinglePY get price: {n_steps / duration} msg/sec')

    print('-' * 100)
    print("ClOrdID Benchmarks")
