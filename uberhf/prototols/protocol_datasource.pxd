from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState


cdef class ProtocolDataSourceBase(ProtocolBase):
    cdef void disconnect_client(self, ConnectionState * cstate) nogil

    cdef int initialize_client(self, ConnectionState * cstate) nogil
