from libc.stdint cimport uint16_t

ctypedef enum ProtocolStatus: UHF_INACTIVE, UHF_CONNECTING, UHF_INITIALIZING, UHF_ACTIVE, UHF_ERROR

cdef extern from "uhfprotocols.h"  nogil:
    const int MODULE_ID_UHFEED
    const int MODULE_ID_ORDER_ROUTER
    const int MODULE_ID_TEST_SRC
    const int MODULE_ID_TEST_FEED


    const uint16_t TRANSPORT_HDR_MGC
    const size_t TRANSPORT_SENDER_SIZE
    const size_t V2_TICKER_MAX_LEN

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
    const char PROTOCOL_ID_DATASOURCE
    const char PROTOCOL_ID_BASE
    const char PROTOCOL_ID_DATAFEED

    #
    # Error codes
    #
    const int PROTOCOL_ERR_GENERIC
    const int PROTOCOL_ERR_SIZE
    const int PROTOCOL_ERR_WRONG_TYPE
    const int PROTOCOL_ERR_WRONG_ORDER
    const int PROTOCOL_ERR_LIFE_ID
    const int PROTOCOL_ERR_CLI_TIMEO
    const int PROTOCOL_ERR_SRV_TIMEO
    const int PROTOCOL_ERR_CLI_ERR
    const int PROTOCOL_ERR_SRV_ERR
    const int PROTOCOL_ERR_ARG_ERR