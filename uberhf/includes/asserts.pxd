from libc.signal cimport raise_, SIGTRAP

cdef extern from "assert.h" nogil:
    # Replacing name to avoid conflict with python assert keyword!
    void cyassert "assert"(bint)


cdef inline int cybreakpoint(bint condition) nogil:
    """
    Breakpoint macro
    """
    return raise_(SIGTRAP) if condition != 0 else 0
