from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport PROTOCOL_ID_DATASOURCE
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE
from uberhf.prototols.protocol_base cimport ProtocolBase


# Set child protocol message types in lower case to avoid conflicts with BaseProtocol
DEF MSGT_INIT = b'i'
DEF MSGT_QUOTE = b'q'
DEF MSGT_IINFO = b'o'


cdef class ProtocolDataSourceBase(ProtocolBase):
    def __cinit__(self, module_id, transport, source_client = None, feed_server = None, heartbeat_interval_sec=5):
        if source_client is None and feed_server is None:
            raise ValueError(f'You must set one of source_client or feed_server')
        elif source_client is not None and feed_server is not None:
            raise ValueError(f'Arguments are mutually exclusive: source_client, feed_server')
        cdef  bint is_server = 0
        #cybreakpoint(1)
        #breakpoint()
        if source_client is not None:
            is_server = 0
            self.source_client = source_client
        elif feed_server is not None:
            is_server = 1
            self.feed_server = feed_server

        # Calling super() class in Cython must be by static
        ProtocolBase.protocol_initialize(self, PROTOCOL_ID_DATASOURCE, is_server, module_id, transport, heartbeat_interval_sec)

    cdef void disconnect_client(self, ConnectionState * cstate) nogil:
        ProtocolBase.disconnect_client(self, cstate)
        if self.is_server:
            self.feed_server.source_on_disconnect(cstate)
        else:
            self.source_client.source_on_disconnect()

    cdef int initialize_client(self, ConnectionState * cstate) nogil:
        return 1

    cdef int activate_client(self, ConnectionState * cstate) nogil:
        return 1

    cdef int on_process_new_message(self, void * msg, size_t msg_size) nogil:
        cdef TransportHeader * hdr = <TransportHeader *> msg
        cdef int rc = 0

        if hdr.protocol_id != self.protocol_id:
            # Protocol doesn't match
            cyassert(0)
            return rc

        # In order from the most frequent to less frequent
        if hdr.msg_type == MSGT_QUOTE:
            cyassert(0)
        elif hdr.msg_type == MSGT_IINFO:
            cyassert(0)
        elif hdr.msg_type == MSGT_INIT:
            # Source initialization request/reply
            cyassert(0)
        else:
            rc = ProtocolBase.on_process_new_message(self, msg, msg_size)
            cyassert(rc != 0)

        return rc








