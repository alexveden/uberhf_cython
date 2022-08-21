from uberhf.includes.hashmap cimport HashMapBase
from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMapBase
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport, TransportHeader
from uberhf.includes.uhfprotocols cimport *

cdef enum SourceStatus:
    inactive = 0
    connecting = 1
    initializing = 2
    active = 3

ctypedef struct HeartbeatConnectMessage:
    TransportHeader header

    SourceStatus sender_status

ctypedef struct SourceState:
    # Keep sender_id as first item to allow HashMapDataSources seek by string
    char sender_id[TRANSPORT_SENDER_SIZE + 1]
    unsigned int foreign_life_id
    SourceStatus status
    long last_heartbeat_time_ns

cdef class HashMapDataSources(HashMapBase):
    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil

cdef class ProtocolDatasourceClient:
    cdef char protocol_id
    cdef Transport transport
    cdef unsigned int client_life_id
    cdef unsigned int server_life_id
    cdef SourceStatus status

    cdef int req_connect_heartbeat(self)
    cdef int on_rep_connect_heartbeat(self, HeartbeatConnectMessage *msg)
    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC

cdef class ProtocolDatasourceServer:
    cdef char protocol_id
    cdef Transport transport
    cdef readonly object core
    cdef unsigned int server_life_id
    cdef HashMapDataSources connected_clients

    cdef int rep_connect_heartbeat(self, SourceState * state) except PROTOCOL_ERR_GENERIC
    cdef int on_req_connect_heartbeat(self, HeartbeatConnectMessage *msg)
    cdef int on_process_new_message(self, void * msg, size_t msg_size) except PROTOCOL_ERR_GENERIC