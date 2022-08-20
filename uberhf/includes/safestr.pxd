# IMPORTANT: dont' forget to include sources in the module .pyx file
#  # distutils: sources = uberhf/includes/hashmap.c uberhf/includes/safestr.c
cdef extern from "safestr.h"  nogil:
    size_t strlcpy(char *dst, const char *src, size_t dsize)