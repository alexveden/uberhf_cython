# distutils: sources = uberhf/includes/safestr.c


cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil:
    """
    Copy string src to buffer dst of size dsize.  At most dsize-1
    chars will be copied.  Always NUL terminates (unless dsize == 0).
    Returns strlen(src); if retval >= dsize, truncation occurred.
    """
    return safe_strcpy(dst, src, dsize)
