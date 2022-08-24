

cdef class UHFeedAbstract:
    """
    Abstract interface for the protocol calls
    """
    cdef source_on_initialize(self):
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        pass

    cdef source_on_initialize_data(self):
        """
        Source sends initialization data
        :return: 
        """
        pass

    cdef source_on_activate(self):
        """
        Source completed initialization and ready to activate
        :return: 
        """
        pass

    cdef void source_on_disconnect(self, ConnectionState * cstate) nogil:
        """
        Source notifies about disconnection
        :return: 
        """
        pass

    cdef source_on_quote(self):
        """
        New quote emitted from source
        :return:
        """
        pass

    cdef source_on_iinfo(self):
        """
        New instrument information from source
        :return: 
        """
        pass

    cdef source_on_status(self):
        """
        Source status has changed
        :return: 
        """
        pass

    cdef feed_initialize(self):
        """
        Data feed client wants to initialize
        :return: 
        """
        pass

    cdef feed_subscribe(self):
        """
        Data feed client wants to subscribe on instrument
        :return: 
        """
        pass

    cdef feed_unsubscribe(self):
        """
        Data feed client wants to unsubscribe on instrument
        :return: 
        """
        pass

    cdef feed_disconnect(self):
        """
        Data feed client notifies about disconnection
        :return: 
        """
        pass