from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE
from uberhf.includes.hashmap cimport HashMapBase
from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.hashmap cimport HashMapBase
from libc.string cimport strcmp, strlen, strcpy
from .transport cimport Transport, TransportHeader

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
    int foreign_life_id
    SourceStatus status
    long last_heartbeat_time_ns

cdef class HashMapDataSources(HashMapBase):
    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil

