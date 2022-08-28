from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport
from uberhf.includes.uhfprotocols cimport *
from uberhf.prototols.protocol_base cimport ProtocolBase, ConnectionState
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, InstrumentInfo


cdef class ProtocolDataSource(ProtocolBase):
    cdef DatasourceAbstract source_client
    cdef UHFeedAbstract feed_server

    cdef void initialize_client(self, ConnectionState * cstate) nogil
    cdef void activate_client(self, ConnectionState * cstate) nogil
    cdef void disconnect_client(self, ConnectionState * cstate) nogil

    cdef int send_register_instrument(self, char * v2_ticker, uint64_t instrument_id, InstrumentInfo * iinfo) nogil
    cdef int on_register_instrument(self, ProtocolDSRegisterMessage *msg) nogil

    cdef int send_new_quote(self, ProtocolDSQuoteMessage * qmsg, int send_no_copy) nogil
    #cdef int on_new_quote(self, ProtocolDSQuoteMessage * msg) nogil

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil
