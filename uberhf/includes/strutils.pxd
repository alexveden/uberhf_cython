cdef extern from "safestr.h"  nogil:
    size_t safe_strcpy "strlcpy"(char *dst, const char *src, size_t dsize)

cdef size_t strlcpy(char * dst, const char * src, size_t  dsize) nogil

