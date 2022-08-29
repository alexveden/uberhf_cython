from uberhf.datafeed.tester_datafeed cimport DataFeedTester
from libc.stdint cimport uint64_t
import zmq
URL_DEALER = b'tcp://localhost:9100'
URL_SUB = b'tcp://localhost:9101'

cpdef main():
    ctx = zmq.Context()
    print(f"Starting DataFEED tester ZMQ_ROUTER->{URL_DEALER}  ZMQ_SUB->{URL_SUB}")
    dst = None
    try:
        n_unique_tickers = 10
        dst = DataFeedTester(<uint64_t> ctx.underlying, URL_DEALER,  URL_SUB)
        print('Starting main loop')
        dst.main()
    except:
        raise
    finally:
        dst.close()

    ctx.destroy(0)