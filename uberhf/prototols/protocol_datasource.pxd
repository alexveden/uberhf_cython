from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract


cdef class ProtocolDataSourceBase(ProtocolBase):
    cdef DatasourceAbstract source_client
    cdef UHFeedAbstract feed_server

    cdef void disconnect_client(self, ConnectionState * cstate) nogil

    cdef int initialize_client(self, ConnectionState * cstate) nogil
