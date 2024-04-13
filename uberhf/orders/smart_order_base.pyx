from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsgStruct, FIXMsg
from libc.stdint cimport uint64_t, uint16_t, int8_t, uint8_t
from .fix_orders cimport FIXNewOrderSingle
from uberhf.datafeed.quotes_cache cimport QCRecord
from uberhf.includes.utils cimport strlcpy
from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP,\
                                      FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
                                      FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
                                      FIX_ET_STATUS, FIX_OS_PFILL, FIXNewOrderSingle


cdef class OrdPosition:
    pass

cdef class SmartOrderBase:
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

        cdef char * smart_clord_id = FIXMsg.get_str(tmp_msg, 11)
        if smart_clord_id == NULL:
            raise ValueError(f'SmartOrder ClOrdId is missing, or error. ErrCode: {FIXMsg.get_last_error(tmp_msg)}')
        self.status = FIX_OS_CREA
        self.smart_clord_id = <bytes>smart_clord_id
        self.orders = {}
        self.position = {}
        self.oms = oms

    def __dealloc__(self):
        if self.smart_msg != NULL:
            FIXMsg.destroy(self.smart_msg)


    cdef int smart_order_new(self):
        """
        Creates new smart order        
        :return: 
        """
        raise NotImplementedError()
        #cdef QCRecord * q = self.quote(b'RU.F.RTS.U22')
        #return self.send(FIXNewOrderSingle.create('test', q, 1010, 200, 1, qty=10))

    cdef int smart_order_load(self):
        """
        Load previously save smart order into working state        
        :return: 
        """
        pass
        # TODO:
        #   1. Reinitialize subscriptions
        #   2. Update QCRecord* pointers of all FIXNewOrderSingle
        #   3. ? Request status of all orders from gate?

    cdef int smart_order_invalidate(self):
        """
        Smart Order raised some unmanageable error, try to stop everything ASAP, 
        including cancelling all orders, and stop responding to events  
        :return: 
        """
        pass
        # TODO:
        #   1. Set a special invalid flag which prevents all actions from child class
        #   2. Cancel all orders
        #   3. Send notification for logging system

    cdef int smart_order_finish(self):
        """
        This method is called when smart order in terminal state and used for cleaning up
        """
        pass

    cdef QCRecord * quote(self, char * v2_ticker):
        """
        Get quote for initial order (also subscribe for updates)
        
        :param v2_ticker: 
        :return: 
        """
        if v2_ticker == NULL:
            return NULL
        return self.oms.quote_get_subscribe(self, v2_ticker, 0, -1)

    cdef int send(self, FIXNewOrderSingle order):
        """
        Sends new single order to the OMS
        
        :param order: 
        :return: 
        """
        assert order.smart_key not in self.orders, f'Duplicate order smart-key, or forgotten order finalization'
        self.orders[order.smart_key] = order
        return self.oms.gate_send_order_new(self, order)

    cdef int cancel(self, FIXNewOrderSingle order):
        """
        Cancels existing single order
        
        :param order: 
        :return: 
        """
        return self.oms.gate_send_order_cancel(self, order)

    cdef int replace(self, FIXNewOrderSingle order, double price, double qty):
        """
        Replaces existing single order
        
        :param order: 
        :param price: 
        :param qty: 
        :return: 
        """
        return self.oms.gate_send_order_replace(self, order, price, qty)

    cdef finalize(self, FIXNewOrderSingle order):
        """
        Finalizes finished order which is in terminal state (canceled, filled, rejected, etc)
        
        :param order: 
        :return: 
        """
        assert order.is_finished() == 1
        del self.orders[order.smart_key]

    def get(self, smart_key: str) -> FIXNewOrderSingle:
        """
        Get active single order from cache
        
        :param smart_key: 
        :return: None if not found
        """
        return self.orders.get(smart_key)

    def has(self, smart_key: str) -> bool:
        """
        Check if FIXNewOrderSingle with `smart_key` is in active cache
        :param smart_key:
        :return:
        """
        return smart_key in self.orders

    def items(self):
        """
        Iterator over active cache, returns key, value tuple
        Example:
            for key, value in self.items():
                pass
        :return:
        """
        return self.orders.items()

    def count(self) -> int:
        """
        Number of active orders
        
        :return:
        """
        return len(self.orders)

    cdef on_quote(self, QCRecord * q):
        """
        Event called every time any subscribed instrument emitted the quote event (the same event may also trigger on_order_quote())
        
        :param q: 
        :return: 
        """
        # TODO: Implement with FIXTester
        pass

    cdef on_order_quote(self, FIXNewOrderSingle o):
        """
        Event called for every order when its instrument (QCRecord * q) emitted quote event
        
        :param o: 
        :return: 
        """
        # TODO: Implement with FIXTester
        pass

    cdef on_order_status(self, FIXNewOrderSingle o):
        """
        Event called when order status has changed (except pending) 
        
        :param o: 
        :return: 
        """
        # TODO: Implement with FIXTester
        pass

    cdef on_order_trade(self, FIXNewOrderSingle o):
        """
        Event called when order execution type (trade) emitted by gate
        
        :param o: 
        :return: 
        """
        # TODO: Implement with FIXTester
        pass

    cdef _on_oms_trade_report(self, QCRecord *q, double trade_qty, double trade_price):
        """
        This method is called by OMS 
        :return: 
        """
        # TODO: Implement with FIXTester
        # Updates smart order position
        return -1



