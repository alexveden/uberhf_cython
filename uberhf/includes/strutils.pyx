# distutils: sources = uberhf/includes/safestr.c

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil:
    return safe_strcpy(dst, src, dsize)