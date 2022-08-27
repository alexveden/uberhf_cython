
from posix.time cimport clock_gettime, timespec, CLOCK_REALTIME, CLOCK_MONOTONIC
from .asserts cimport cyassert
from posix.time cimport timespec, nanosleep
from libc.string cimport strlen

cdef extern from "safestr.h"  nogil:
    size_t safe_strcpy "strlcpy"(char *dst, const char *src, size_t dsize)

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil

cdef inline bint is_str_valid(char * s, size_t max_buf_size) nogil:
    """
    Checks if char* is not NULL, non zero-len, and fits max_buf_size (including \0 char)
    
    :param s: string 
    :param max_buf_size: buffer for a string 
    :return: 
    """
    if s == NULL:
        return 0
    cdef size_t _slen = strlen(s)
    return _slen > 0 and _slen < max_buf_size

cdef extern from "utils.h"  nogil:
    const double TIMEDELTA_NANO
    const double TIMEDELTA_MICRO
    const double TIMEDELTA_MILLI
    const double TIMEDELTA_SEC
    const double TIMEDELTA_MIN
    const double TIMEDELTA_HOUR
    const double TIMEDELTA_DAY

cdef long datetime_nsnow() nogil

cdef long datetime_from_spec(timespec *spec) nogil

cdef long timer_nsnow() nogil

cdef double timedelta_ns(long dt_end, long dt_begin, double timedelta_units) nogil

cdef void random_seed(unsigned int seed) nogil

cdef double random_float() nogil

cdef int random_int(int lo, int hi) nogil

cdef int sleep_ns(double seconds) nogil

cdef unsigned int gen_lifetime_id(int module_id) nogil