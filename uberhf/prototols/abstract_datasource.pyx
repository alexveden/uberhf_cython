
cdef class DatasourceAbstract:
    cdef source_send_initialize(self):
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        pass

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

    cdef void source_on_disconnect(self, ConnectionState * cstate) nogil:
        """
        Source notifies about disconnection
        :return: 
        """
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