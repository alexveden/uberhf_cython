from libc.stdint cimport uint64_t


cdef class DatasourceAbstract:
    cdef int source_on_initialize(self) nogil:
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        return 1

    cdef int source_on_register_instrument(self, char * v2_ticker, uint64_t instrument_id, int error_code, int instrument_index) nogil:
        return -1

    cdef source_send_activate(self):
        """
        Source completed initialization and ready to activate
        :return: 
        """
        pass

    cdef source_send_disconnect(self):
        pass

    cdef void source_on_disconnect(self) nogil:
        return

    cdef void source_on_activate(self) nogil:
        pass


    cdef source_send_quote(self):
        """
        New quote emitted from source
        :return:
        """
        pass

    cdef source_send_iinfo(self):
        """
        New instrument information from source
        :return: 
        """
        pass

    cdef source_send_status(self):
        """
        Source status has changed
        :return: 
        """
        pass