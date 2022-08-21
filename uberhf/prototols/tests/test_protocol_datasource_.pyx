import time
import unittest
import zmq
from libc.stdint cimport uint64_t
from uberhf.prototols.transport cimport *
from uberhf.prototols.libzmq cimport *
from uberhf.includes.uhfprotocols cimport *
from uberhf.includes.utils cimport datetime_nsnow, TIMEDELTA_SEC, timedelta_ns, TIMEDELTA_MICRO
from uberhf.prototols.protocol_datasource cimport ProtocolDatasourceClient, ProtocolDatasourceServer

from unittest.mock import MagicMock
URL_BIND = b'tcp://*:7100'
URL_CONNECT = b'tcp://localhost:7100'


class CyProtocolDatasourceTestCase(unittest.TestCase):

    def test_protocol_init_server(self):
        with zmq.Context() as ctx:
            transport_s = Transport(<uint64_t> ctx.underlying, URL_BIND, ZMQ_ROUTER, b'SRV')
            transport_c = Transport(<uint64_t> ctx.underlying, URL_CONNECT , ZMQ_DEALER, b'CLI')

            ps = ProtocolDatasourceServer(transport_s, MagicMock())
            pc = ProtocolDatasourceServer(transport_c, MagicMock())
            try:
                pass
            finally:
                transport_s.close()
                transport_c.close()
