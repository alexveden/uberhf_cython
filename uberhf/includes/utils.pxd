from libc.stdlib cimport rand, srand, RAND_MAX
from posix.time cimport clock_gettime, timespec, CLOCK_REALTIME, CLOCK_MONOTONIC
from .asserts cimport cyassert
from posix.time cimport timespec, nanosleep
from libc.time cimport time, tm, localtime, time_t

cdef extern from "safestr.h"  nogil:
    size_t safe_strcpy "strlcpy"(char *dst, const char *src, size_t dsize)

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil

cdef extern from "utils.h"  nogil:
    const double TIMEDELTA_NANO
    const double TIMEDELTA_MICRO
    const double TIMEDELTA_MILLI
    const double TIMEDELTA_SEC
    const double TIMEDELTA_MIN
    const double TIMEDELTA_HOUR
    const double TIMEDELTA_DAY

cdef inline long datetime_nsnow() nogil:
    """
    Microseconds from epoch, compatible with pd.Timestamp(now)
    
    :return: timestamp in nanoseconds (10**-9 sec)
    """
    cdef timespec spec
    cyassert(clock_gettime(CLOCK_REALTIME, &spec) == 0)
    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef inline long datetime_from_spec(timespec *spec) nogil:
    """
    Makes timestamp from timespec structure

    :return: timestamp in nanoseconds (10**-9 sec)
    """
    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef inline long timer_nsnow() nogil:
    """
    High precision (ns) timer timestamp (starts from system startup time and not affected by NTP, or clock changes)
    
    :return: timestamp in nanoseconds (10**-9 sec)
    """
    cdef timespec spec
    cyassert(clock_gettime(CLOCK_MONOTONIC, &spec) == 0)
    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef inline double timedelta_ns(long dt_end, long dt_begin, double timedelta_units):
    """
    Time delta between two nanosecond timestamps (dt_end-dt_begin) / timedelta_units
    
    :param dt_end: end time
    :param dt_begin: begin time
    :param timedelta_units: for converting nanosecond to time span
    :return: 
    """
    return (<double>(dt_end - dt_begin)) / timedelta_units

cdef inline void random_seed(unsigned int seed) nogil:
    """
    Seeds the random generator
    :param seed: 
    :return: 
    """
    srand(seed)

cdef inline  double random_float() nogil:
    """
    Random float number between (0, 1) (both not included) 
    :return: 
    """
    # On linux rand() function the same as random()
    return <double>rand() / <double>RAND_MAX

cdef inline int random_int(int lo, int hi) nogil:
    """
    Random integer number between range of integers [lo; hi) 
    :return: 
    """
    # On linux rand() function the same as random()
    return rand() % (hi - lo) + lo

cdef inline int sleep_ns(double seconds):
    """
    Possible to do a fractional sleep in seconds
    :param seconds: 
    :return: 
    """
    cdef timespec tim, tim2;

    tim.tv_sec = <int>seconds
    tim.tv_nsec = <long>((seconds - tim.tv_sec) * 10**9)
    return nanosleep(&tim, &tim2)

cdef inline unsigned int gen_lifetime_id(int module_id):
    """
    Unique unsigned int number, which represents module lifetime, i.e. the moment when it was started
    
    Number format mmHHMMSSrr, where mm - module ID, HHMMSS - hour:min:sec, rr - some random number between 1 and 99
    :param module_id: unique module ID number (between 1 and 40)
    :return: Integer in format mmHHMMSSrr
    """
    cyassert(module_id > 0 and module_id <= 40)

    cdef timespec spec
    cyassert(clock_gettime(CLOCK_REALTIME, &spec) == 0)

    random_seed(spec.tv_nsec)

    cdef tm *curr_time = localtime(&spec.tv_sec)
    cdef int rnd = random_int(1, 99)

    # 4294967295= mmHHMMSSrr
    cdef result =  100000000 * module_id + 1000000 * curr_time.tm_hour + 10000 * curr_time.tm_min + 100 * curr_time.tm_sec + rnd
    return result
