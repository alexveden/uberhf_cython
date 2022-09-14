from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsgStruct, FIXMsg
from libc.stdint cimport uint64_t, uint16_t, int8_t, uint8_t
from .fix_orders cimport FIXNewOrderSingle
from uberhf.datafeed.quotes_cache cimport QCRecord
from uberhf.includes.utils cimport strlcpy

cdef class OrdPosition:
    cdef readonly bytes v2_ticker
    cdef readonly long exchange_ticker_id
    cdef readonly int ticker_index
    cdef readonly double qty
    cdef readonly double value
    cdef readonly double fixed_point_value
    cdef readonly double realized_pnl
    cdef readonly double costs

    cdef process_report(self, int * rep_id):
        pass

    @staticmethod
    cdef OrdPosition from_execution_report(int * rep_id):
        cdef OrdPosition p = OrdPosition()
        p.qty = 100

cdef class SmartOrderBase:
    cdef OMSAbstract oms
    cdef FIXMsgStruct * smart_msg
    cdef readonly bytes clord_id
    cdef readonly dict orders       # type: Dict[uint64_t, SingleOrder]
    cdef readonly dict position     # type: Dict[v2_ticker, OrdPosition]
    cdef readonly char status
    cdef readonly double qty
    cdef readonly double qty_fill

    def __cinit__(self, OMSAbstract oms, uint64_t fixmsg_ptr):
        """
        Initializing core elements of the Smart Order

        :param oms: UHF OMS class for interfacing calls
        :param fixmsg_ptr: a pointer to a  FIXMsgStruct from a socket or previously saved data
        :return:
        """
        cdef FIXMsgStruct * tmp_msg = <FIXMsgStruct *>fixmsg_ptr

        if not FIXMsg.is_valid(tmp_msg):
            raise RuntimeError(f'Corrupted initial message!')

        self.smart_msg = FIXMsg.copy(tmp_msg)
        if self.smart_msg == NULL:
            raise RuntimeError(f'Memory error?!')

        self.orders = {}
        self.position = {}



    cdef create_new(self):
        """
        Creates new smart order
        
        :return: 
        """
        cdef QCRecord q
        strlcpy(q.v2_ticker, b'01234578', 20)
        q.ticker_index = 10
        q.instrument_id = 123

        cdef FIXNewOrderSingle ord = FIXNewOrderSingle.create(&q, 1010, 200, 1, qty=10)
        self.orders[123] = ord
        # Send new orders
        #self.oms.gate_send_new_single()



    def __dealloc__(self):
        pass
