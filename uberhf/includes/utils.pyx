# distutils: sources = uberhf/includes/safestr.c
from posix.time cimport clock_gettime, timespec, CLOCK_REALTIME
from .asserts cimport cyassert

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil:
    """
    Copy string src to buffer dst of size dsize.  At most dsize-1
    chars will be copied.  Always NUL terminates (unless dsize == 0).
    Returns strlen(src); if retval >= dsize, truncation occurred.
    """
    return safe_strcpy(dst, src, dsize)

cdef long datetime_nsnow() nogil:
    """
    Nanoseconds from epoch
    """
    cdef timespec spec
    cyassert(clock_gettime(CLOCK_REALTIME, &spec) == 0)
    return spec.tv_nsec