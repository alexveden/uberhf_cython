from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen, memset, strncpy
from uberhf.prototols.libzmq cimport *
from zmq.error import ZMQError
from libc.stdint cimport uint64_t
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport PROTOCOL_ID_DATASOURCE
from uberhf.includes.utils cimport strlcpy, datetime_nsnow, gen_lifetime_id
from uberhf.includes.asserts cimport cyassert
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE
from uberhf.prototols.protocol_base cimport ProtocolBase

DEF MSGT_INIT = b'I'

cdef class ProtocolDataSourceBase(ProtocolBase):
    def __cinit__(self, is_server, module_id, transport, heartbeat_interval_sec=5):
        # Calling super() class in Cython must be by static
        ProtocolBase.protocol_initialize(self, is_server, module_id, transport, heartbeat_interval_sec)

    cdef void disconnect_client(self, ConnectionState * cstate) nogil:
        ProtocolBase.disconnect_client(self, cstate)

    cdef int initialize_client(self, ConnectionState * cstate) nogil:
        # Send notification to the core that client is going to be initialized!
        return ProtocolBase.initialize_client(self, cstate)










