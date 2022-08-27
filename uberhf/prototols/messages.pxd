from libc.stdint cimport uint64_t, uint16_t
from uberhf.includes.uhfprotocols cimport *

ctypedef struct Quote:
    long last_upd_utc
    double bid
    double ask
    double last
    double bid_size
    double ask_size

ctypedef struct InstrumentInfo:
    double theo_price
    double tick_size
    double min_lot_size
    int price_scale
    double margin_req
    bint usd_point_value

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
    InstrumentInfo iinfo
    int error_code
    int instrument_index

ctypedef struct ProtocolDSQuoteMessage:
    TransportHeader header
    uint64_t instrument_id
    int instrument_index
    bint is_snapshot
    Quote quote

ctypedef struct ProtocolDFSubscribeMessage:
    TransportHeader header
    char v2_ticker[V2_TICKER_MAX_LEN]
    bint is_subscribe
    int instrument_index

ctypedef struct ProtocolDFUpdateMessage:
    TransportHeader header
    uint64_t instrument_id
    int instrument_index
    int update_type

ctypedef struct ProtocolDFStatusMessage:
    TransportHeader header
    char data_source_id[TRANSPORT_SENDER_SIZE]
    ProtocolStatus quote_status