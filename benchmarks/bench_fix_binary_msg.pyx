from uberhf.orders.fix_binary_msg cimport FIXBinaryMsg
from uberhf.includes.utils cimport timer_nsnow, timedelta_ns, TIMEDELTA_SEC, random_int
from uberhf.includes.asserts cimport cybreakpoint, cyassert
from libc.stdlib cimport calloc, free, realloc, malloc
from libc.string cimport memset, memmove
from libc.stdint cimport uint16_t
from libc.limits cimport USHRT_MAX
from bisect import bisect_left
from cpython cimport array
import array
import time


cdef int rnd_array[100]
rnd_array = [ 629, 1574, 8407, 4136, 3012, 4371, 2613, 1576, 4968, 2367, 3160,
       9330, 2231, 6958, 4545, 5648, 2134, 1644, 4568, 4196, 9325, 1292,
       7449,  932, 3944, 4544, 8256, 2811, 8740, 1683, 2098, 1270, 4299,
       6629, 1537, 9760, 7299, 4444, 8096, 5058, 7178, 1512, 5625, 5444,
       8786, 9222, 7783,  336, 4081, 2862, 1773, 3077, 2058, 8112, 7052,
       7473, 1032,  165,  429, 5298, 2792, 6973, 1269, 7433, 5798, 8885,
       7434, 1298, 2422, 1712, 7733, 5693, 4637, 8607, 4721, 1136, 8695,
       7101, 3681, 6409, 5534, 1671, 1204, 4491, 2192, 3082, 9174, 7384,
       2102, 4858, 1795, 4032, 7711, 9536, 1294, 2230, 5856, 7797, 4313,
       5908]

cdef fixmsg_make_sequential(int n_elements):
    """
    HASHMAP Based:
        (DEBUG)FixMsg speed: 26461.628141217538 msg/sec (100 tags/msg)
        (PROD) FixMsg speed: 174662.3113804233 msg/sec
    
    BINARY Tree Based:
        (DEBUG) FixMsg speed: 37615.44443204305 msg/sec
        (PROD)  FixMsg speed: 270493.1346813719 msg/sec
    
    ONLY SET:
    BINARY:
        (DEBUG)FixMsg speed: 68357.37327715324 msg/sec

    :param n_elements: 
    :return: 
    """
    cdef FIXBinaryMsg m = FIXBinaryMsg.__new__(FIXBinaryMsg, <char>b'C', 2000)
    cdef int i =0
    cdef void* value
    cdef uint16_t value_size
    cdef int rc
    #
    for i in range(n_elements):
        if i+1 == 35:
            continue
        rc = m.set_int(i+1, i)
        assert rc > 0, rc

    for i in range(n_elements):
        if i+1 == 35:
            continue
        value = m.get_int(i+1)
        assert value != NULL, m.get_last_error_str(m.get_last_error())

cdef fixmsg_fixed_random_tags(int n_elements, int * rnd_array, int rnd_size):
    """
    BINARY Tree random
    (PROD) FixMsg speed: 128457.2750139874 msg/sec
    (DEBUG) FixMsg speed: 28227.863135997435 msg/sec
    
    ONLY SET:
    (DEBUG) FixMsg speed: 49112.4788183404 msg/sec

    :return: 
    """

    cdef FIXBinaryMsg m = FIXBinaryMsg.__new__(FIXBinaryMsg, <char> b'C', 2000)
    cdef int i = 0
    cdef void * value
    cdef uint16_t value_size
    cdef int rc

    for i in range(rnd_size):
        rc = m.set_int(rnd_array[i], rnd_array[i])
        assert rc > 0, rc

    for i in range(rnd_size):
        value = m.get_int(rnd_array[i])
        assert value != NULL, m.get_last_error_str(m.get_last_error())

cpdef main():
    cdef int msg_size = 0
    cdef int n_steps = 1000000
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
    assert duration != 0
    print(f'FixMsg speed: {n_steps/duration} msg/sec')


    print(f'RANDOM ACCESS {msg_size} tags/msg')
    t_start = timer_nsnow()
    for i in range(n_steps):
        fixmsg_fixed_random_tags(msg_size, rnd_array, 100)
    t_end = timer_nsnow()
    duration = timedelta_ns(t_end, t_start, TIMEDELTA_SEC)
    print(f'FixMsg speed: {n_steps/duration} msg/sec')
