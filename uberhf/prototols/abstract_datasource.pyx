from libc.stdint cimport uint64_t
from uberhf.includes.asserts cimport cyassert

cdef class DatasourceAbstract:
    cdef void register_datasource_protocol(self, object protocol):
        raise NotImplementedError(f'You must implement this method in child class')

    cdef void source_on_initialize(self) nogil:
        return

    cdef void source_on_disconnect(self) nogil:
        return

    cdef void source_on_activate(self) nogil:
        return

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil:
        return -1

