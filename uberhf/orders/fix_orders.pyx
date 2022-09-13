from uberhf.datafeed.quote_info import QuoteInfo
from uberhf.includes.utils cimport datetime_nsnow
from libc.math cimport isfinite, NAN
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
                                  double qty,
                                  double target_price = NAN,
                                  char order_type = b'2',
                                  char time_in_force = b'0',
                                  ):
        if qty == 0:
            raise FIXMessageError(38, b'Zero qty')
        if not isfinite(qty):
            raise FIXMessageError(38, b'qty is nan')
        if not isfinite(price):
            # Zero price is allowed for spreads!
            raise FIXMessageError(44, b'price is nan')

        cdef FIXNewOrderSingle self = FIXNewOrderSingle()
        self.q = q
        self.price = price
        self.qty = abs(qty)
        self.cum_qty = 0
        self.leaves_qty = 0
        self.side = 1 if qty > 0 else -1
        self.status = FIX_OS_CREA
        self.clord_id = 0
        self.orig_clord_id = 0
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
        rc = FIXMsg.set_char(self.msg, 54, b'1' if qty > 0 else b'2')
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
        return NULL
        # cdef FIXBinaryMsg cxl_msg = FIXBinaryMsg(<char> b'F', 200, 10)
        #
        # # Tag 11: ClOrdID
        # cdef int clord_len = len(req_clord_id)
        # if clord_len == 0 or clord_len >= 20:
        #     raise FIXMessageError(11, f'clord_id length must be > 0 and < 20 chars, got {clord_len}')
        # rc = cxl_msg.set_str(11, req_clord_id, clord_len)
        # if rc <= 0:
        #     raise FIXMessageError(11, cxl_msg.get_last_error_str(rc))
        #
        # # Tag 38: Order Qty
        # rc = cxl_msg.set_double(38, self.qty)
        # if rc <= 0:
        #     raise FIXMessageError(38, cxl_msg.get_last_error_str(rc))
        #
        # # Tag 41: OrigClOrdID
        # cdef char * orig_clord = self.msg.get_str(11)
        # if orig_clord == NULL:
        #     raise FIXMessageError(41, self.msg.get_last_error_str(self.msg.get_last_error()))
        # rc = cxl_msg.set_str(41, orig_clord, clord_len)
        # if rc <= 0:
        #     if rc == -20:
        #         # ERR_UNEXPECTED_TYPE_SIZE
        #         # Highly likely len(orig_clord) != len(req_clord_id)
        #         #  This is unexpected behaviour, all ClOrdIDs must be with the same len
        #         raise FIXMessageError(41, b'Possible len(orig_clord) != len(req_clord_id), all ClOrdID must have the same length')
        #     else:
        #         raise FIXMessageError(41, cxl_msg.get_last_error_str(rc))
        #
        # # Tag 54: Side
        # cdef char * side = self.msg.get_char(54)
        # if side == NULL:
        #     raise FIXMessageError(54, self.msg.get_last_error_str(self.msg.get_last_error()))
        # cxl_msg.set_char(54, side[0])
        #
        # # Tag 60: Transact time
        # rc = cxl_msg.set_utc_timestamp(60, datetime_nsnow())
        # if rc <= 0:
        #     raise FIXMessageError(60, cxl_msg.get_last_error_str(rc))
        #
        # # TODO: set instrument info
        # # Tag 55: Symbol set to v2 symbol? -- it's going to be stable
        # # Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        # # Tag 22: SecurityIDSource - quote cache ticker index - may be dynamic and actual during short time (not reliable but fast!)
        #
        # return cxl_msg

    cdef int register(self, uint64_t clord_id, uint64_t orig_clord_id):
        """
        Registers new order action

        - New order placements (clord_id, 0)
        - Cancel/replaces      (new_clord, clord_id)  

        :param clord_id: 
        :param orig_clord_id: 
        :return: 
        """
        cdef int rc = 0
        if self.clord_id == 0:
            # New order

            assert (orig_clord_id == 0)  # Expected always to be zero for new orders
            if clord_id == 0:
                return -3  # ERR_FIX_VALUE_ERROR

            # Keep initial ClOrdId in the FIX Msg, and it won't change after replaces
            rc = FIXMsg.replace(self.msg, 11, &clord_id, sizeof(uint64_t), b'L')
            if rc <= 0:
                return rc

            self.clord_id = clord_id
            return 1
        else:
            if self.orig_clord_id != 0:
                # Something is pending now, it's not allowed to replace
                return -23  # DEF ERR_STATE_TRANSITION       = -23
            if self.clord_id != orig_clord_id:
                # When replacing/cancelling orig_clord_id must match
                return -3  # ERR_FIX_VALUE_ERROR
            if clord_id == 0:
                # Bad value
                return -3  # ERR_FIX_VALUE_ERROR

            # We don't change FIX msg clord_id, it will remain as ID for the whole order lifetime
            self.orig_clord_id = self.clord_id
            self.clord_id = clord_id
            return 1

    cdef int process_execution_report(self, FIXMsgStruct * m):
        assert (self.clord_id != 0) # Must be registered

        return 1

    def __dealloc__(self):
        if self.msg != NULL:
            FIXMsg.destroy(self.msg)