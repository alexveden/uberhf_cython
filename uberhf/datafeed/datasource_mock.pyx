from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSourceBase
from libc.stdint cimport uint64_t


cdef class DataSourceMock(DatasourceAbstract):

    cdef void register_protocol(self, ProtocolDataSourceBase protocol):
        self.protocol = protocol

    cdef int source_on_initialize(self) nogil:
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        self.protocol.send_register_instrument(b'RU.F.RTS', 1234)
        return 1

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil:
        if error_code == 0 and instrument_index >= 0:
            return self.protocol.send_activate()
        else:
            return -100000



