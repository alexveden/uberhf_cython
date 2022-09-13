from .abstract_oms cimport OMSAbstract
from .fix_msg cimport FIXMsg, FIXMsgStruct
from libc.stdint cimport uint64_t
from libc.math cimport NAN, isnan
from math import isfinite
from uberhf.orders.fix_orders cimport FIX_OS_CREA, FIX_OS_NEW, FIX_OS_FILL, FIX_OS_DFD, FIX_OS_CXL, FIX_OS_PCXL, FIX_OS_STP, FIX_OS_REJ, FIX_OS_SUSP,\
                                      FIX_OS_PNEW, FIX_OS_CALC, FIX_OS_EXP, FIX_OS_ACCPT, FIX_OS_PREP, FIX_ET_NEW, FIX_ET_DFD, FIX_ET_CXL, FIX_ET_REP, \
                                      FIX_ET_PCXL, FIX_ET_STP, FIX_ET_REJ, FIX_ET_SUSP, FIX_ET_PNEW, FIX_ET_CALC, FIX_ET_EXP, FIX_ET_PREP, FIX_ET_TRADE, \
                                      FIX_ET_STATUS, FIXNewOrderSingle


cdef class FIXMsgC:
    def __cinit__(self, uint64_t msg_ptr):
        self.m = <FIXMsgStruct*>msg_ptr

    def __dealloc__(self):
        if self.m != NULL:
            FIXMsg.destroy(self.m)
            self.m = NULL

cdef class FIXTester(OMSAbstract):
    def __cinit__(self):
        self._clord_id_cnt = 0

    cdef int order_register_single(self, FIXNewOrderSingle order):
        cdef uint64_t clord_id = self.order_gen_clord_id()

        assert order.clord_id == 0
        rc = order.register(clord_id, 0)
        assert rc == 1, f'o.register(clord_id, 0) failed, reason: {rc}'
        assert order.clord_id == clord_id
        return 1

    cdef uint64_t order_gen_clord_id(self):
        self._clord_id_cnt += 1
        return self._clord_id_cnt

    cdef FIXMsgC fix_exec_report_msg(self,
                             FIXNewOrderSingle order,
                             uint64_t clord_id,
                             char exec_type,
                             char ord_status,
                             double cum_qty = NAN,
                             double leaves_qty = NAN,
                             double last_qty = NAN,
                             double price = NAN,
                             double order_qty = NAN,
                             uint64_t orig_clord_id = 0,
                            ):
        assert order.clord_id > 0, 'Unregistered order!'

        cdef FIXMsgStruct * m = FIXMsg.create(b'8', 300, 30)
        assert clord_id > 0
        assert FIXMsg.set_uint64(m, 11,  clord_id) == 1

        if orig_clord_id > 0:
            # TAG 41 Orig Clord ID - must be set explicitly
            assert FIXMsg.set_uint64(m, 41, orig_clord_id) == 1

        # TAG 150 Exec type
        assert FIXMsg.set_char(m, 150, exec_type) == 1

        # TAG 39 Ord status
        assert FIXMsg.set_char(m, 39, ord_status) == 1

        # TAG 38 Qty
        if isnan(order_qty):
            order_qty = order.qty
        else:
            assert exec_type == b'5', f'Only applicable to exec_type=5 (replace)'
            assert order_qty > 0
        assert FIXMsg.set_double(m, 38, order_qty) == 1

        # TAG 38 CumQty
        if isnan(cum_qty):
            cum_qty = order.cum_qty
        else:
            assert cum_qty <= order.qty
            assert cum_qty >= 0
        assert FIXMsg.set_double(m, 14, cum_qty) == 1

        # TAG 38 LeavesQty
        if isnan(leaves_qty):
            leaves_qty = order.leaves_qty
        else:
            assert leaves_qty >= 0
            assert leaves_qty <= order.qty
        assert FIXMsg.set_double(m, 151, leaves_qty) == 1
        assert cum_qty + leaves_qty <= order_qty, f'cum_qty[{cum_qty}] + leaves_qty[{leaves_qty}] <= order_qty[{order_qty}]'

        if not isnan(last_qty):
            assert not isnan(leaves_qty), f'Must also set leaves_qty, when trade'
            assert not isnan(cum_qty), f'Must also set cum_qty, when trade'
            assert exec_type == b'F', 'Only applicable to exec_type=F (trade)'
            assert last_qty > 0
            assert FIXMsg.set_double(m, 32, last_qty) == 1

            assert last_qty == cum_qty-order.cum_qty, f'Probably incorrect Trade qty'
        else:
            assert exec_type != b'F', 'You must set last_qty when exec_type=F (trade)'


        if not isnan(price):
            assert exec_type == b'5', f'Only applicable to exec_type=5 (replace)'
            assert FIXMsg.set_double(m, 44, price) == 1

        msg_c = FIXMsgC(<uint64_t>m)
        return msg_c






