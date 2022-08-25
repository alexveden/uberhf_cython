from libc.stdint cimport uint64_t
from uberhf.includes.asserts cimport cyassert

cdef class UHFeedAbstract:
    """
    Abstract interface for the protocol calls
    """
    cdef void register_datasource_protocol(self, object protocol):
        raise NotImplementedError('You must override this method in child class')

    cdef void source_on_initialize(self, char * source_id) nogil:
        """
        Indicates that source wants to re-initialize its feed
        :return: 
        """
        return

    cdef void source_on_activate(self, char * source_id) nogil:
        """
        Source completed initialization and ready to activate
        :return: 
        """
        return

    cdef void source_on_disconnect(self, char * source_id) nogil:
        """
        Source notifies about disconnection
        :return: 
        """
        return

    cdef int source_on_register_instrument(self, char * source_id, char * v2_ticker, uint64_t instrument_id) nogil:
        """
        Source sends initialization data
        :return: 
        """
        return -1