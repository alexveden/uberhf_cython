from uberhf.datafeed.uhffeed cimport Quote
from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.uhfprotocols cimport *

ctypedef struct TransportHeader:
    uint16_t magic_number
    char sender_id[TRANSPORT_SENDER_SIZE]
    char protocol_id
    char msg_type
    unsigned int server_life_id
    unsigned int client_life_id


ctypedef struct ProtocolBaseMessage:
    TransportHeader header
    ProtocolStatus status

ctypedef struct ProtocolDSRegisterMessage:
    TransportHeader header
    char v2_ticker[V2_TICKER_MAX_LEN]
    uint64_t instrument_id
    int error_code
    int instrument_index

ctypedef struct ProtocolDSQuoteMessage:
    TransportHeader header
    uint64_t instrument_id
    int instrument_index
    bint is_snapshot
    Quote quote
