from uberhf.datafeed.tester_datasource cimport DataSourceTester
from libc.stdint cimport uint64_t
import zmq
URL_DEALER = b'tcp://localhost:9100'

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
        dst.close()

    ctx.destroy(0)