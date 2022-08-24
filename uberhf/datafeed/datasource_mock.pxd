from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSourceBase
from libc.stdint cimport uint64_t


cdef class DataSourceMock(DatasourceAbstract):
    cdef ProtocolDataSourceBase protocol

    cdef void register_protocol(self, ProtocolDataSourceBase protocol)

    cdef int source_on_initialize(self) nogil

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil



