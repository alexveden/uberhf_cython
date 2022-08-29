import time
from libc.stdlib cimport malloc, free
from libc.string cimport strcmp
import unittest
import zmq
from libc.stdint cimport uint64_t
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.prototols.protocol_base cimport ProtocolBase,  ProtocolBaseMessage, ConnectionState
from uberhf.prototols.protocol_datasource cimport ProtocolDataSource
from uberhf.prototols.messages cimport ProtocolDSRegisterMessage, ProtocolDSQuoteMessage, InstrumentInfo
from uberhf.prototols.abstract_uhfeed cimport UHFeedAbstract
from uberhf.prototols.abstract_datasource cimport DatasourceAbstract
from uberhf.includes.utils cimport strlcpy
from uberhf.includes.hashmap cimport HashMap
from .quotes_cache cimport SharedQuotesCache
import numpy as np
cimport numpy as np
from libc.stdio cimport printf

cdef bint global_is_shutting_down = 0

cdef class DataSourceTester(DatasourceAbstract):
    cdef Transport transport_dealer
    cdef ProtocolDataSource protocol
    cdef int on_initialize_ncalls
    cdef int on_disconnect_ncalls
    cdef int on_activate_ncalls
    cdef int on_register_n_ok
    cdef int on_register_n_err
    cdef size_t n_unique_tickers
    cdef HashMap hm_tickers
    cdef int quotes_sent
    cdef int quotes_sent_errors

    cdef bint is_shutting_down
    cdef int zmq_poll_timeout
    cdef zmq_pollitem_t zmq_poll_array[1]

    cdef void register_datasource_protocol(self, object protocol)

    cdef void source_on_initialize(self) nogil
    cdef void source_on_disconnect(self) nogil
    cdef void source_on_activate(self) nogil

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil

    cdef int main(self) nogil

    cdef void benchmark_quotes(self, int n_quotes) nogil
