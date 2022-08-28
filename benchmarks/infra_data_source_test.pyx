from uberhf.datafeed.uhfeed cimport UHFeed
from libc.stdint cimport uint64_t
import zmq
URL_ROUTER = b'tcp://*:7100'
URL_DEALER = b'tcp://localhost:7100'

URL_PUB = b'tcp://*:7101'
URL_SUB = b'tcp://localhost:7101'


from uberhf.datafeed.datasource_tester cimport DataSourceTester
from libc.stdint cimport uint64_t
import zmq
URL_ROUTER = b'tcp://*:7100'
URL_DEALER = b'tcp://localhost:7100'

URL_PUB = b'tcp://*:7101'
URL_SUB = b'tcp://localhost:7101'


cpdef main():
    ctx = zmq.Context()
    print(f"Starting DataSource tester at: {URL_DEALER}")
    dst = None
    try:
        n_unique_tickers = 10
        dst = DataSourceTester(<uint64_t> ctx.underlying, URL_DEALER,n_unique_tickers )
        print('Starting main loop')
        dst.main()
    except:
        raise
    finally:
        if dst is not None:
            dst.transport_dealer.close()

    ctx.destroy(0)