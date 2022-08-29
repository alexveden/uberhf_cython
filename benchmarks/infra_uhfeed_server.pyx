from uberhf.datafeed.uhfeed cimport UHFeed
from libc.stdint cimport uint64_t
import zmq
URL_ROUTER = b'tcp://*:9100'
URL_PUB = b'tcp://*:9101'

cpdef main():
    ctx = zmq.Context()
    print(f"Starting UHFeed ZMQ_ROUTER->{URL_ROUTER}  ZMQ_PUB->{URL_PUB}")
    uhf = None
    try:
        uhf = UHFeed(<uint64_t> ctx.underlying, URL_ROUTER, URL_PUB,  source_capacity=5, quote_capacity=10000)
        print('Starting main loop')
        uhf.main()
    except:
        raise
    finally:
        if uhf is not None:
            uhf.transport_pub.close()
            uhf.transport_router.close()

    ctx.destroy(0)