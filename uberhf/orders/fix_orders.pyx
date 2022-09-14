from uberhf.datafeed.quote_info import QuoteInfo
from uberhf.includes.utils cimport datetime_nsnow
from libc.math cimport isfinite, NAN, isnan
from uberhf.includes.asserts cimport cyassert


class FIXMessageError(Exception):
    def __init__(self, tag, message):
        self.tag = tag
        super().__init__(f'Tag={tag}: {message}')


cdef class FIXNewOrderSingle:

    @staticmethod
    cdef FIXNewOrderSingle create(QCRecord * q,
                                  int account_id,
                                  double price,
                                  int side,
                                  double qty,
                                  double target_price = NAN,
                                  char order_type = b'2',
                                  char time_in_force = b'0',
                                  ):
        if qty <= 0:
            raise FIXMessageError(38, b'Zero or negative qty')
        if not isfinite(qty):
            raise FIXMessageError(38, b'qty is nan')
        if not isfinite(price):
            # Zero price is allowed for spreads!
            raise FIXMessageError(44, b'price is nan')
        if side != -1 and side != 1:
            raise FIXMessageError(54, f'side must be -1 or 1, got {side}')

        cdef FIXNewOrderSingle self = FIXNewOrderSingle()
        self.q = q
        self.price = price
        self.qty = qty
        self.cum_qty = 0
        self.leaves_qty = 0
        self.side = side
        self.status = 0  # Will be set at self.register()!
        self.clord_id = 0
        self.orig_clord_id = 0
        self.ord_type = order_type
        if not isfinite(target_price):
            self.target_price = price
        else:
            self.target_price = target_price

        self.msg = FIXMsg.create(<char> b'D', 157, 11)
        if self.msg == NULL:
            raise FIXMessageError(0, 'Error creating FIXMsg buffer, possible memory error')
        # IMPORTANT: adding in sequential order for the best performance

        # Tag 1: Account
        rc = FIXMsg.set_int(self.msg, 1, account_id)
        if rc <= 0:
            raise FIXMessageError(1, FIXMsg.get_last_error_str(rc))

        # Tag 11: ClOrdID
        rc = FIXMsg.set_uint64(self.msg, 11, 0)
        if rc <= 0:
            raise FIXMessageError(11, FIXMsg.get_last_error_str(rc))

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        #   it may be dynamic and actual during short time (not reliable but fast!)
        rc = FIXMsg.set_int(self.msg, 22, q.ticker_index)
        if rc <= 0:
            raise FIXMessageError(22, FIXMsg.get_last_error_str(rc))

        # Tag 38: Order Qty
        rc = FIXMsg.set_double(self.msg, 38, abs(qty))
        if rc <= 0:
            raise FIXMessageError(38, FIXMsg.get_last_error_str(rc))

        # Tag 40: Order Type
        rc = FIXMsg.set_char(self.msg, 40, order_type)
        if rc <= 0:
            raise FIXMessageError(40, FIXMsg.get_last_error_str(rc))


        # Tag 44: Order Price
        rc = FIXMsg.set_double(self.msg, 44, price)
        if rc <= 0:
            raise FIXMessageError(44, FIXMsg.get_last_error_str(rc))

        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        rc = FIXMsg.set_uint64(self.msg, 48, q.instrument_id)
        if rc <= 0:
            raise FIXMessageError(48, FIXMsg.get_last_error_str(rc))

        # Tag 54: Side
        rc = FIXMsg.set_char(self.msg, 54, b'1' if side > 0 else b'2')
        if rc <= 0:
            raise FIXMessageError(54, FIXMsg.get_last_error_str(rc))

        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        rc = FIXMsg.set_str(self.msg, 55, q.v2_ticker, 0)
        if rc <= 0:
            raise FIXMessageError(55, FIXMsg.get_last_error_str(rc))

        # Tag 59: Time in force
        rc = FIXMsg.set_char(self.msg, 59, time_in_force)
        if rc <= 0:
            raise FIXMessageError(59, FIXMsg.get_last_error_str(rc))

        # Tag 60: Transact time
        rc = FIXMsg.set_utc_timestamp(self.msg, 60, datetime_nsnow())
        if rc <= 0:
            raise FIXMessageError(60, FIXMsg.get_last_error_str(rc))

        return self

    cdef FIXMsgStruct * cancel_req(self):
        cdef int rc = 0
        self.last_fix_error = 1
        rc = self.can_cancel()
        if rc <= 0:
            # Something wrong with order state
            self.last_fix_error = rc
            return NULL

        if self.orig_clord_id != 0:
            # DEF ERR_STATE_TRANSITION       = -23
            self.last_fix_error = -23
            return NULL

        cdef FIXMsgStruct * cxl_req_msg = FIXMsg.create(<char> b'F', 133, 8)
        if cxl_req_msg == NULL:
            self.last_fix_error = -7 # DEF ERR_MEMORY_ERROR           = -7
            return NULL

        # Tag 11: ClOrdID
        rc = FIXMsg.set_uint64(cxl_req_msg, 11, 0)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        #   it may be dynamic and actual during short time (not reliable but fast!)
        rc = FIXMsg.set_int(cxl_req_msg, 22, self.q.ticker_index)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 38: Order Qty
        rc = FIXMsg.set_double(cxl_req_msg, 38, self.qty)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 41: OrigClOrdID
        rc = FIXMsg.set_uint64(cxl_req_msg, 41, self.clord_id)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        rc = FIXMsg.set_uint64(cxl_req_msg, 48, self.q.instrument_id)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 54: Side
        rc = FIXMsg.set_char(cxl_req_msg, 54, b'1' if self.side > 0 else b'2')
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        rc = FIXMsg.set_str(cxl_req_msg, 55, self.q.v2_ticker, 0)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 60: Transact time
        rc = FIXMsg.set_utc_timestamp(cxl_req_msg, 60, datetime_nsnow())
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        return cxl_req_msg

    cdef FIXMsgStruct * replace_req(self, double price, double qty):
        cdef int rc = 1
        self.last_fix_error = 1
        rc = self.can_replace()
        if rc <= 0:
            # Something wrong with order state
            self.last_fix_error = rc
            return NULL
        # Try to avoid excess isnan() func calls
        rc = 0
        if isnan(price) or price == self.price:
            price = self.price
            rc += 1
        if isnan(qty) or qty == self.qty or qty == 0:
            qty = self.qty
            rc += 1

        if rc == 2:
            # Both price/qty are not passed, makes no sens
            self.last_fix_error = -3 # DEF ERR_FIX_VALUE_ERROR        = -3
            return NULL

        rc = 1
        if self.orig_clord_id != 0:
            # DEF ERR_STATE_TRANSITION       = -23
            self.last_fix_error = -23
            return NULL

        cdef FIXMsgStruct * cxl_req_msg = FIXMsg.create(<char> b'G', 154, 10)
        if cxl_req_msg == NULL:
            self.last_fix_error = -7  # DEF ERR_MEMORY_ERROR           = -7
            return NULL

        # Tag 11: ClOrdID
        rc = FIXMsg.set_uint64(cxl_req_msg, 11, 0)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 22: SecurityIDSource - quote cache ticker index
        #   it may be dynamic and actual during short time (not reliable but fast!)
        rc = FIXMsg.set_int(cxl_req_msg, 22, self.q.ticker_index)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 38: Order Qty
        rc = FIXMsg.set_double(cxl_req_msg, 38, qty)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 40: OrdType
        rc = FIXMsg.set_char(cxl_req_msg, 40, self.ord_type)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 41: OrigClOrdID
        rc = FIXMsg.set_uint64(cxl_req_msg, 41, self.clord_id)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 44: Order Price
        rc = FIXMsg.set_double(cxl_req_msg, 44, price)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        rc = FIXMsg.set_uint64(cxl_req_msg, 48, self.q.instrument_id)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 54: Side
        rc = FIXMsg.set_char(cxl_req_msg, 54, b'1' if self.side > 0 else b'2')
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # <INSTRUMENT> Tag 55: Symbol set to v2 symbol
        rc = FIXMsg.set_str(cxl_req_msg, 55, self.q.v2_ticker, 0)
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        # Tag 60: Transact time
        rc = FIXMsg.set_utc_timestamp(cxl_req_msg, 60, datetime_nsnow())
        if rc <= 0:
            self.last_fix_error = rc
            return NULL

        return cxl_req_msg

    cdef int register(self, FIXMsgStruct * msg, uint64_t clord_id, char ord_status):
        """
        Registers new order action

        - New order placements (clord_id, FIX_OS_CREA)
        - Cancel     (new_clord, FIX_OS_PCXL)
        - Replace    (new_clord, FIX_OS_PREP)

        :param clord_id: 
        :param ord_status:  new order status 
        :return: 
        """
        cdef int rc = 0
        if self.clord_id == 0:
            # New order
            if clord_id == 0:
                return -3  # ERR_FIX_VALUE_ERROR
            if self.status != 0:
                # Order status must be = 0!
                return -23 # ERR_STATE_TRANSITION       = -23
            if ord_status != FIX_OS_CREA:
                return -23  # ERR_STATE_TRANSITION       = -23

            if self.msg != msg:
                # New order must pass the self.msg
                return -4 #DEF ERR_FIX_NOT_ALLOWED        = -4

            # Keep initial ClOrdId in the FIX Msg, and it won't change after replaces
            rc = FIXMsg.replace(msg, 11, &clord_id, sizeof(uint64_t), b'L')
            if rc <= 0:
                return rc
            self.status = ord_status
            self.clord_id = clord_id
            return 1
        else:
            if self.orig_clord_id != 0:
                # Something is pending now, it's not allowed to replace
                return -23  # DEF ERR_STATE_TRANSITION       = -23
            if clord_id == 0:
                # Bad value
                return -3  # ERR_FIX_VALUE_ERROR

            if self.msg == msg:
                # Cancel/replace order must pass the other than self.msg
                return -4 #DEF ERR_FIX_NOT_ALLOWED        = -4

            if ord_status == FIX_OS_PCXL:
                if not self.can_cancel():
                    return -23  # DEF ERR_STATE_TRANSITION       = -23
                self.status = FIX_OS_PCXL
            elif ord_status == FIX_OS_PREP:
                if not self.can_replace():
                    return -23  # DEF ERR_STATE_TRANSITION       = -23
                self.status = FIX_OS_PREP
            else:
                return -23  # DEF ERR_STATE_TRANSITION       = -23

            # Put new ClOrdID into the message, assuming that old one is already in tag 41
            rc = FIXMsg.replace(msg, 11, &clord_id, sizeof(uint64_t), b'L')
            if rc <= 0:
                return rc

            # Message already must contain OrigClOrdID == self.clord_id
            cyassert(FIXMsg.get_uint64(msg, 41) != NULL and FIXMsg.get_uint64(msg, 41)[0] == self.clord_id)

            # We don't change FIX msg clord_id, it will remain as ID for the whole order lifetime
            self.orig_clord_id = self.clord_id
            self.clord_id = clord_id
            return 1

    @staticmethod
    cdef char change_status(char status, char fix_msg_type, char msg_exec_type, char msg_status):
        """
        FIX Order Strate transition algo
        
        :param status: current order status 
        :param fix_msg_type: incoming/or requesting order type,  these are supported:
                '8' - execution report, 
                '9' - Order Cancel reject,
                'F' - Order cancel request (for checking if possible to cancel current order)
                'G' -  Order replace request (for checking if possible to replace current order)
        :param msg_exec_type: (only for execution report), for other should be 0
        :param msg_status: new fix msg order status, or required status
        :return: positive if state transition is possible, 
                 zero if transition is valid, but need to wait for a good state
                 negative on error, error -23 means FIX order status is not expected
        """
        if fix_msg_type == b'8':  # Execution report
            if status == FIX_OS_CREA:
                # CREATED -> (PendingNew, Rejected)
                if msg_status == FIX_OS_PNEW:
                    return FIX_OS_PNEW
                elif msg_status == FIX_OS_REJ:
                    return FIX_OS_REJ
                else:
                    return -23 #ERR_STATE_TRANSITION
            elif status == FIX_OS_PNEW:
                # PendingNew -> (Rejected, New, Filled, Canceled)
                if msg_status == FIX_OS_REJ:
                    return FIX_OS_REJ
                elif msg_status == FIX_OS_NEW:
                    return FIX_OS_NEW
                elif msg_status == FIX_OS_FILL:
                    return FIX_OS_FILL
                elif msg_status == FIX_OS_PFILL:
                    return FIX_OS_PFILL
                elif msg_status == FIX_OS_CXL:
                    return FIX_OS_CXL
                elif msg_status == FIX_OS_SUSP:
                    return FIX_OS_SUSP
                else:
                    return -23 #ERR_STATE_TRANSITION
            elif status == FIX_OS_NEW:
                # New -> (Rejected, New, Suspended, PartiallyFilled, Filled, Canceled, Expired, DFD)
                if msg_status == FIX_OS_PNEW or msg_status == FIX_OS_CREA or msg_status == FIX_OS_ACCPT:
                    return -23 #ERR_STATE_TRANSITION
                elif msg_status == FIX_OS_NEW:
                    # Reinstatement, allowed but not trigger state change
                    return 0
                return msg_status
            elif status == FIX_OS_FILL or status == FIX_OS_CXL or \
                    status == FIX_OS_REJ or status == FIX_OS_EXP:
                # Order in terminal state - no status change allowed!
                return 0
            elif status == FIX_OS_SUSP:
                # Order algorithmically was suspended
                if msg_status == FIX_OS_NEW:
                    return FIX_OS_NEW
                elif msg_status == FIX_OS_PFILL:
                    return FIX_OS_PFILL
                elif msg_status == FIX_OS_CXL:
                    return FIX_OS_CXL
                elif msg_status == FIX_OS_SUSP:
                    # Possible duplidates or delayed fills
                    return 0
                else:
                    return -23 #ERR_STATE_TRANSITION
            elif status == FIX_OS_PFILL:
                if msg_status == FIX_OS_FILL:
                    return FIX_OS_FILL
                elif msg_status == FIX_OS_PFILL:
                    return FIX_OS_PFILL
                elif msg_status == FIX_OS_PREP:
                    return FIX_OS_PREP
                elif msg_status == FIX_OS_PCXL:
                    return FIX_OS_PCXL
                elif msg_status == FIX_OS_CXL:
                    return FIX_OS_CXL
                elif msg_status == FIX_OS_EXP:
                    return FIX_OS_EXP
                elif msg_status == FIX_OS_SUSP:
                    return FIX_OS_SUSP
                elif msg_status == FIX_OS_STP:
                    return FIX_OS_STP

                else:
                    return -23 #ERR_STATE_TRANSITION
            elif status == FIX_OS_PCXL:
                if msg_status == FIX_OS_CXL:
                    return FIX_OS_CXL
                elif msg_status == FIX_OS_CREA:
                    return -23  #ERR_STATE_TRANSITION
                else:
                    return 0
            elif status == FIX_OS_PREP:
                if msg_exec_type == FIX_ET_REP:
                    # Successfully replaced
                    if msg_status == FIX_OS_NEW or msg_status == FIX_OS_PFILL \
                            or msg_status == FIX_OS_FILL or msg_status == FIX_OS_CXL:
                        return msg_status
                    else:
                        return -23  #ERR_STATE_TRANSITION
                else:
                    if msg_status == FIX_OS_CREA or msg_status == FIX_OS_ACCPT:
                        return -23  #ERR_STATE_TRANSITION
                    else:
                        # Technically does not count any status,
                        # until get replace reject or exec_type = FIX_ET_REP
                        return 0

            else:
                return -23 #ERR_STATE_TRANSITION

        elif fix_msg_type == b'9': # Order Cancel reject
            cyassert(msg_exec_type == 0)
            if msg_status == FIX_OS_CREA or msg_status == FIX_OS_ACCPT:
                return -23  #ERR_STATE_TRANSITION
            return msg_status
        elif fix_msg_type == b'F' or fix_msg_type == b'G':
            cyassert(msg_exec_type == 0)
            # 'F' - Order cancel request (order requests self cancel)
            # 'G' -  Order replace request (order requests self change)
            if status == FIX_OS_PCXL or status == FIX_OS_PREP:
                # Status is pending, we must wait
                return 0
            elif status == FIX_OS_NEW or status == FIX_OS_SUSP or status == FIX_OS_PFILL:
                # Order is active and good for cancel/replacement
                return status
            else:
                return -23 #ERR_STATE_TRANSITION
        else:
            return -4 # ERR_FIX_NOT_ALLOWED

    cdef int is_finished(self):
        """
        Check if order is in terminal state (no subsequent changes expected)
        
        :return: 1 on success, 0 if active or will be able to activate later, < 0 - on error
        """
        if self.status == FIX_OS_FILL or self.status == FIX_OS_CXL or self.status == FIX_OS_REJ or self.status == FIX_OS_EXP:
            return 1
        else:
            return 0

    cdef int can_cancel(self):
        """
        Check if order can be canceled from its current state
        
        :return: 0 if already pending cancel/replace, >=1 - good to cancel, -23 - incorrect order transition
        """
        return <int>FIXNewOrderSingle.change_status(self.status, b'F', 0, FIX_OS_PCXL)

    cdef int can_replace(self):
        """
        Check if order can be replaced from its current state
        
        :return: 0 if already pending cancel/replace, >=1 - good to cancel, -23 - incorrect order transition
        """
        return <int>FIXNewOrderSingle.change_status(self.status, b'G', 0, FIX_OS_PREP)

    cdef int process_cancel_rej_report(self, FIXMsgStruct * m):
        if m.header.msg_type != b'9':
            return -3 # DEF ERR_FIX_VALUE_ERROR        = -3

        cdef char * order_status = FIXMsg.get_char(m, 39)
        if order_status == NULL:
            return -3 # DEF ERR_FIX_VALUE_ERROR        = -3

        cdef char new_status = FIXNewOrderSingle.change_status(self.status,
                                                               m.header.msg_type,
                                                               0,
                                                               order_status[0])
        #
        if order_status[0] == FIX_OS_REJ:
            # Very weird (emergency) case, because the ClOrdId does not exist
            #   Let's set order inactive
            self.leaves_qty = 0

        if new_status > 0:
            self.status = new_status
            return 1
        else:
            return new_status


    cdef int process_execution_report(self, FIXMsgStruct * m):
        assert (self.clord_id != 0) # Must be registered
        if m.header.msg_type != b'8':
            return -3 # DEF ERR_FIX_VALUE_ERROR        = -3

        cdef uint64_t * orig_clord_id
        cdef uint64_t * clord_id = FIXMsg.get_uint64(m, 11)
        if clord_id == NULL:
            return -3 # DEF ERR_FIX_VALUE_ERROR        = -3

        cdef double * cum_qty = FIXMsg.get_double(m, 14)  # tag 14: cum qty
        if cum_qty == NULL:
            return FIXMsg.get_last_error(m)

        cdef char * order_status = FIXMsg.get_char(m, 39)
        if order_status == NULL:
            return FIXMsg.get_last_error(m)

        if clord_id[0] != self.clord_id:
            if clord_id[0] != self.orig_clord_id:
                return -4  # ERR_FIX_NOT_ALLOWED        = -4

        cdef char * exec_type = FIXMsg.get_char(m, 150)
        if exec_type == NULL:
            return FIXMsg.get_last_error(m)



        cdef double * leaves_qty = FIXMsg.get_double(m, 151) # tag 151: leaves qty
        if leaves_qty == NULL:
            return FIXMsg.get_last_error(m)


        cdef char new_status = FIXNewOrderSingle.change_status(self.status,
                                                               m.header.msg_type,
                                                               exec_type[0],
                                                               order_status[0])

        self.leaves_qty = leaves_qty[0]
        self.cum_qty = cum_qty[0]

        cdef double * new_price
        cdef double * order_qty

        if exec_type[0] == FIX_ET_REP:
            # Order has been successfully replaced
            new_price = FIXMsg.get_double(m, 44)
            if new_price != NULL:
                # Price may not be in execution report, it's not an error
                self.price = new_price[0]

            order_qty = FIXMsg.get_double(m, 38)  # tag 38: order qty
            if order_qty != NULL:
                # Qty may not be in execution report, it's not an error
                self.qty = order_qty[0]

            # Clearing OrigOrdId to allow subsequent order changes
            self.orig_clord_id = 0

        if new_status > 0:
            self.status = new_status
            return 1
        else:
            return new_status


    def __dealloc__(self):
        if self.msg != NULL:
            FIXMsg.destroy(self.msg)