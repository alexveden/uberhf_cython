from libc.stdint cimport uint64_t
from uberhf.includes.uhfprotocols cimport V2_TICKER_MAX_LEN, TRANSPORT_SENDER_SIZE, ProtocolStatus

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


ctypedef struct UHFeedRecord:
    char v2_ticker[V2_TICKER_MAX_LEN]
    uint64_t instrument_id
    char data_source_id[TRANSPORT_SENDER_SIZE]
    unsigned int data_source_life_id
    ProtocolStatus client_status
    Quote quote
    InstrumentInfo iinfo



