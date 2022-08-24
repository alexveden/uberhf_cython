
cdef class DatasourceAbstract:
    cdef int source_client_initialize(self) nogil:
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        return 1

    cdef source_send_initialize_data(self):
        """
        Source sends initialization data
        :return: 
        """
        pass

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