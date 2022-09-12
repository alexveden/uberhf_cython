# distutils: sources = uberhf/includes/safestr.c
from libc.stdlib cimport rand, srand, RAND_MAX
cimport cython
from .asserts cimport cyassert
from libc.time cimport tm, localtime
from libc.limits cimport INT_MIN

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil:
    """
    Copy string src to buffer dst of size dsize.  At most dsize-1
    chars will be copied.  Always NUL terminates (unless dsize == 0).
    Returns strlen(src); if retval >= dsize, truncation occurred.
    """
    return safe_strcpy(dst, src, dsize)

cdef long datetime_nsnow() nogil:
    """
    Microseconds from epoch, compatible with pd.Timestamp(now)

    :return: timestamp in nanoseconds (10**-9 sec)
    """
    cdef timespec spec
    cdef int rc = clock_gettime(CLOCK_REALTIME, &spec)
    cyassert(rc == 0)
    cyassert(spec.tv_sec > 0)
    cyassert(spec.tv_nsec > 0)
    return spec.tv_sec  * 1000000000 + spec.tv_nsec

cdef long datetime_from_spec(timespec *spec) nogil:
    """
    Makes timestamp from timespec structure

    :return: timestamp in nanoseconds (10**-9 sec)
    """
    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef long timer_nsnow() nogil:
    """
    High precision (ns) timer timestamp (starts from system startup time and not affected by NTP, or clock changes)

    :return: timestamp in nanoseconds (10**-9 sec)
    """
    cdef timespec spec
    cdef int rc = clock_gettime(CLOCK_MONOTONIC, &spec)
    cyassert(rc == 0)
    cyassert(sizeof(long) == 8)

    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef double timedelta_ns(long dt_end, long dt_begin, double timedelta_units) nogil:
    """
    Time delta between two nanosecond timestamps (dt_end-dt_begin) / timedelta_units

    :param dt_end: end time
    :param dt_begin: begin time
    :param timedelta_units: for converting nanosecond to time span
    :return: 
    """
    return (<double> (dt_end - dt_begin)) / timedelta_units

cdef void random_seed(unsigned int seed) nogil:
    """
    Seeds the random generator
    :param seed: 
    :return: 
    """
    srand(seed)

@cython.cdivision(True)
cdef double random_float() nogil:
    """
    Random float number between (0, 1) (both not included) 
    :return: 
    """
    # On linux rand() function the same as random()
    return <double> rand() / <double> RAND_MAX

@cython.cdivision(True)
cdef int random_int(int lo, int hi) nogil:
    """
    Random integer number between range of integers [lo; hi)
         
    :return: random int between lo(including) and hi(excluding), or INT_MIN - on error  
    """
    cyassert(hi >= lo)

    if hi == lo:
        return lo

    if hi < lo:
        return INT_MIN

    # On linux rand() function the same as random()
    return rand() % (hi - lo) + lo

cdef int sleep_ns(double seconds) nogil:
    """
    Possible to do a fractional sleep in seconds
    :param seconds: 
    :return: 
    """
    cdef timespec tim, tim2;

    tim.tv_sec = <int> seconds
    tim.tv_nsec = <long> ((seconds - tim.tv_sec) * 10 ** 9)
    return nanosleep(&tim, &tim2)

cdef unsigned int gen_lifetime_id(int module_id) nogil:
    """
    Unique unsigned int number, which represents module lifetime, i.e. the moment when it was started

    Number format mmHHMMSSrr, where mm - module ID, HHMMSS - hour:min:sec, rr - some random number between 1 and 99
    :param module_id: unique module ID number (between 1 and 40)
    :return: Integer in format mmHHMMSSrr, or zero on error!
    """
    cyassert(module_id > 0 and module_id <= 40)
    if not( module_id > 0 and module_id <= 40):
        return 0


    cdef timespec spec
    cdef int rc = clock_gettime(CLOCK_REALTIME, &spec)
    if rc != 0:
        return 0

    random_seed(spec.tv_nsec)

    cdef tm *curr_time = localtime(&spec.tv_sec)
    cdef int rnd = random_int(1, 99)
    cyassert(rnd >= 1 and rnd <= 99)

    # 4294967295= mmHHMMSSrr
    return 100000000 * module_id + 1000000 * curr_time.tm_hour + 10000 * curr_time.tm_min + 100 * curr_time.tm_sec + rnd
