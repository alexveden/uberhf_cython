from uberhf.orders.fix_binary_msg cimport FIXOffsetMap, FIXTagHashMap, FIXBinaryMsg
from uberhf.includes.utils cimport timer_nsnow, timedelta_ns, TIMEDELTA_SEC, random_int
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from libc.stdlib cimport calloc, free, realloc, malloc
from libc.string cimport memset, memmove
from libc.stdint cimport uint16_t
from libc.limits cimport USHRT_MAX
from bisect import bisect_left


cdef fixmsg_make_sequential(int n_elements):
    """
    HASHMAP Based:
        (DEBUG)FixMsg speed: 26461.628141217538 msg/sec (100 tags/msg)
        (PROD) FixMsg speed: 174662.3113804233 msg/sec
        

    :param n_elements: 
    :return: 
    """
    cdef FIXBinaryMsg m = FIXBinaryMsg.__new__(FIXBinaryMsg, <char>b'C', 0)
    cdef int i =0
    cdef void* value
    cdef uint16_t value_size
    #
    for i in range(n_elements):
        m.set(i+1, &i, sizeof(int), b'i')

    for i in range(n_elements):
        m.get(i+i, &value, &value_size, b'i')

cpdef main():
    cdef int msg_size = 0
    cdef int n_steps = 10000
    cdef long t_start
    cdef long t_end
    cdef double duration
    cdef int i = 0

    msg_size = 100
    print('-'*100)
    print(f'SEQUENTIAL ACCESS {msg_size} tags/msg')
    t_start = timer_nsnow()
    for i in range(n_steps):
        fixmsg_make_sequential(msg_size)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'FixMsg speed: {n_steps/duration} msg/sec')

