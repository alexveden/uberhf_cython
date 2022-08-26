from libc.stdint cimport uint64_t
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN, TRANSPORT_SENDER_SIZE, ProtocolStatus
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.protocol_datasource cimport ProtocolDataSourceBase


#
#
# cdef class UHFeed(UHFeedAbstract):
#     cdef ProtocolDataSourceBase protocol
#
#     cdef void register_datasource_protocol(self, object protocol)
#
#     cdef void source_on_initialize(self, char * source_id) nogil
#     cdef void source_on_activate(self, char * source_id) nogil
#     cdef void source_on_disconnect(self, char * source_id) nogil
#
#     cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id) nogil
#
#     cdef void source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil
