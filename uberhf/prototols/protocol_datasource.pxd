from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract



ctypedef struct ProtocolDSRegisterMessage:
    TransportHeader header
    char v2_ticker[V2_TICKER_MAX_LEN]
    uint64_t instrument_id
    int error_code
    int instrument_index


cdef class ProtocolDataSourceBase(ProtocolBase):
    cdef DatasourceAbstract source_client
    cdef UHFeedAbstract feed_server

    cdef void disconnect_client(self, ConnectionState * cstate) nogil
    cdef int initialize_client(self, ConnectionState * cstate) nogil
    cdef int activate_client(self, ConnectionState * cstate) nogil

    cdef int send_register_instrument(self, char * v2_ticker, uint64_t instrument_id) nogil
    cdef int on_register_instrument(self, ProtocolDSRegisterMessage *msg) nogil

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
