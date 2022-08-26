from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMap
from libc.string cimport strcmp, strlen, strcpy
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE, V2_TICKER_MAX_LEN, ProtocolStatus, TRANSPORT_HDR_MGC
from uberhf.prototols.messages cimport Quote, InstrumentInfo, ProtocolDSQuoteMessage



ctypedef struct QCHeader:
    uint16_t magic_number
    unsigned int uhffeed_life_id
    int quote_count
    int quote_capacity
    int source_count
    int source_capacity
    int quote_errors
    int source_errors

ctypedef struct QCSourceHeader:
    char data_source_id[TRANSPORT_SENDER_SIZE]
    unsigned int data_source_life_id
    ProtocolStatus quotes_status
    int instruments_registered
    int quotes_processed
    int iinfo_processed
    int quote_errors
    int source_errors
    long last_quote_ns
    uint16_t magic_number

ctypedef struct QCRecord:
    char v2_ticker[V2_TICKER_MAX_LEN]
    uint64_t instrument_id
    char data_source_id[TRANSPORT_SENDER_SIZE]
    int data_source_hidx
    Quote quote
    InstrumentInfo iinfo
    int magic_number

ctypedef struct Name2Idx:
    char name[V2_TICKER_MAX_LEN]
    int idx


cdef class SharedQuotesCache:
    cdef bint is_server
    cdef int lock_fd
    cdef bint lock_acquired
    cdef int shmem_fd
    cdef void * mmap_data
    cdef size_t mmap_size
    cdef unsigned int uhffeed_life_id
    cdef HashMap source_map
    cdef HashMap ticker_map

    cdef QCHeader * header
    cdef QCSourceHeader * sources
    cdef QCRecord * records

    cdef int source_initialize(self, char * data_src_id, unsigned int data_source_life_id) nogil
    cdef int source_register_instrument(self, char * data_src_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfo iinfo) nogil
    cdef int source_activate(self, char * data_src_id) nogil
    cdef int source_disconnect(self, char * data_src_id) nogil
    cdef int source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil

    cdef QCRecord * get(self, char * v2_ticker) nogil
    cdef QCSourceHeader * get_source(self, char * data_source_id) nogil

    cdef close(self)

    @staticmethod
    cdef size_t calc_shmem_size(int source_capacity, int quotes_capacity)
    cdef void _reload_sources_or_srvreset(self) nogil
    cdef void _reload_quotes(self) nogil

    @staticmethod
    cdef void reset_quote(Quote *q) nogil


