from libc.stdint cimport uint16_t

cdef extern from "uhfprotocols.h"  nogil:
    const uint16_t TRANSPORT_HDR_MGC
    const size_t TRANSPORT_SENDER_SIZE

    const int TRANSPORT_ERR_OK
    const int TRANSPORT_ERR_ZMQ
    const int TRANSPORT_ERR_BAD_SIZE
    const int TRANSPORT_ERR_BAD_HEADER
    const int TRANSPORT_ERR_BAD_PARTSCOUNT
    const int TRANSPORT_ERR_SOCKET_CLOSED
    const int TRANSPORT_ERR_NULL_DATA
    const int TRANSPORT_ERR_NULL_DEALERID

    #
    # Unique protocol IDs
    #
    const char PROTOCOL_ID_NONE
    const char PROTOCOL_ID_TEST

    #
    # Error codes
    #
    const int PROTOCOL_ERR_GENERIC
    const int PROTOCOL_ERR_SIZE
    const int PROTOCOL_ERR_WRONG_TYPE

    #
    # MSG Types
    #
    const char PROTOCOL_MSGT_HEARTBEAT