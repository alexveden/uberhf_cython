from uberhf.datafeed.quotes_cache cimport QCRecord


cdef class OMSAbstract:
    cdef QCRecord * get_quote_subscribe(self, char * v2_ticker, long ticker_id, int ticker_index)

    cdef int gate_send_new_single(self)
    cdef int gate_on_execution_report(self)
    cdef int gate_on_status(self)

    cdef int strategy_on_new(self)
    cdef int strategy_send_execution_report(self)
    cdef int strategy_send_cxlrej_report(self)
    cdef int strategy_on_cancel_request(self)
    cdef int strategy_on_replace_request(self)


    cdef void feed_on_quote(self, int instrument_index) nogil
    cdef void feed_on_instrumentinfo(self, int instrument_index) nogil


