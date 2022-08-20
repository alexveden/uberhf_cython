from libc.stdlib cimport rand, srand, RAND_MAX
from posix.time cimport clock_gettime, timespec, CLOCK_REALTIME, CLOCK_REALTIME_COARSE
from .asserts cimport cyassert

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
    Nanoseconds from epoch
    """
    cdef timespec spec
    # CLOCK_REALTIME_COARSE - works much faster than CLOCK_REALTIME
    #cyassert(clock_gettime(CLOCK_REALTIME, &spec) == 0)
    cyassert(clock_gettime(CLOCK_REALTIME, &spec) == 0)
    return spec.tv_sec * 1000000000 + spec.tv_nsec

cdef inline double timedelta_ns(long d1, long d2, double timedelta_units):
    return (<double>(d1 - d2)) / timedelta_units

cdef inline void random_seed(unsigned int seed) nogil:
    """
    Seeds the random generator
    :param seed: 
    :return: 
    """
    srand(seed)

cdef inline  double random_float() nogil:
    """
    Random float number between 0 and 1 
    :return: 
    """
    return <double>rand() / <double>RAND_MAX