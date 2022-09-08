from uberhf.datafeed.quote_info import QuoteInfo
from uberhf.orders.fix_binary_msg cimport FIXBinaryMsg
from uberhf.includes.utils cimport datetime_nsnow
from libc.math cimport isfinite

class FIXMessageError(Exception):
    def __init__(self, tag, message):
        self.tag = tag
        super().__init__(f'Tag={tag}: {message}')

cdef class FIXNewOrderSinglePy:
    def __init__(self,
                 clord_id: bytes,
                 account: bytes,
                 q: QuoteInfo,
                 price: float,
                 qty: float,
                 order_type: bytes = b'2',
                 time_in_force: bytes = b'0'):
        self.clord_id = clord_id
        self.account = account
        self.q = q
        self.px = price
        self.qty = qty

cdef class FIXNewOrderSingle:
    def __init__(self,
                 clord_id: bytes,
                 account: bytes,
                 q: QuoteInfo,
                 price: float,
                 qty: float,
                 order_type: bytes = b'2',
                 time_in_force: bytes = b'0'):
        cdef int rc = 0
        cdef double _qty = qty
        cdef double _price = price
        cdef int clord_len = len(clord_id)
        if clord_len == 0 or clord_len >= 20:
            raise FIXMessageError(11, f'clord_id length must be > 0 and < 20 chars, got {clord_len}')
        if _qty == 0:
            raise FIXMessageError(38, b'Zero qty')
        if not isfinite(_qty):
            raise FIXMessageError(38, b'qty is nan')
        if not isfinite(_price):
            # Zero price is allowed for spreads!
            raise FIXMessageError(44, b'price is nan')

        self.msg = FIXBinaryMsg(<char>b'D', 200, 10)
        self.q = q
        self._clord_cached = None
        self._price_cached = None
        self._qty_cached = None
        self._side_cached = None

        # Add in sequential order, it's 3x faster than unordered

        # Tag 1: Account
        rc = self.msg.set_str(1, account, 0)
        if rc <= 0: raise FIXMessageError(1, self.msg.get_last_error_str(rc))

        # Tag 11: ClOrdID

        rc = self.msg.set_str(11, clord_id, clord_len)
        if rc <= 0: raise FIXMessageError(11, self.msg.get_last_error_str(rc))

        # Tag 38: Order Qty

        rc = self.msg.set_double(38, abs(_qty))
        if rc <= 0: raise FIXMessageError(38, self.msg.get_last_error_str(rc))

        # Tag 40: Order Type
        rc = self.msg.set_char(40, <char>(order_type[0]))
        if rc <= 0:  raise FIXMessageError(40, self.msg.get_last_error_str(rc))

        # Tag 44: Order Price
        rc = self.msg.set_double(44, _price)
        if rc <= 0: raise FIXMessageError(44, self.msg.get_last_error_str(rc))

        # Tag 54: Side
        rc = self.msg.set_char(54, b'1' if _qty > 0 else b'2')
        if rc <= 0:
            raise FIXMessageError(54, self.msg.get_last_error_str(rc))

        # Tag 59: Time in force
        rc = self.msg.set_char(59, <char>(time_in_force[0]))
        if rc <= 0:  raise FIXMessageError(59, self.msg.get_last_error_str(rc))

        # Tag 60: Transact time
        rc = self.msg.set_utc_timestamp(60, datetime_nsnow())
        if rc <= 0: raise FIXMessageError(60, self.msg.get_last_error_str(rc))

        # TODO: set instrument info
        # Tag 55: Symbol set to v2 symbol? -- it's going to be stable
        # Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        # Tag 22: SecurityIDSource - quote cache ticker index - may be dynamic and actual during short time (not reliable but fast!)


    @property
    def clord_id(self) -> bytes:
        cdef char * clord_id
        if self._clord_cached is None:
            # This is a copy operation, so caching helps to increase speed up to 20x!
            clord_id = self.msg.get_str(11)
            if clord_id == NULL:
                raise FIXMessageError(11, self.msg.get_last_error_str(self.msg.get_last_error()))
            self._clord_cached = <bytes>clord_id
        return self._clord_cached

    @property
    def price(self) -> float:
        # This is a copy operation, so caching helps to increase speed up to 20x!
        cdef double * px
        if self._price_cached is None:
            px = self.msg.get_double(44)
            if px == NULL:
                raise FIXMessageError(44, self.msg.get_last_error_str(self.msg.get_last_error()))
            self._price_cached = px[0]
        return self._price_cached

    @property
    def qty(self) -> float:
        cdef double * qty
        if self._qty_cached is None:
            # Tag 38: Order Qty
            qty = self.msg.get_double(38)
            if qty == NULL:
                raise FIXMessageError(38, self.msg.get_last_error_str(self.msg.get_last_error()))
            self._qty_cached= qty[0]
        return self._qty_cached

    @property
    def side(self) -> int:
        cdef char * side
        if self._side_cached is None:
            # Tag 54: Side
            side = self.msg.get_char(54)
            if side == NULL:
                raise FIXMessageError(54, self.msg.get_last_error_str(self.msg.get_last_error()))
            if side == b'1':
                self._side_cached = 1
            elif side == b'2':
                self._side_cached = -1
            else:
                raise FIXMessageError(54, f'Unsupported FIX side value {side}')

        return self._side_cached

    cpdef FIXBinaryMsg cancel_req(self, bytes req_clord_id):
        cdef FIXBinaryMsg cxl_msg = FIXBinaryMsg(<char> b'F', 200, 10)

        # Tag 11: ClOrdID
        cdef int clord_len = len(req_clord_id)
        if clord_len == 0 or clord_len >= 20:
            raise FIXMessageError(11, f'clord_id length must be > 0 and < 20 chars, got {clord_len}')
        rc = cxl_msg.set_str(11, req_clord_id, clord_len)
        if rc <= 0: raise FIXMessageError(11, cxl_msg.get_last_error_str(rc))

        # Tag 38: Order Qty
        rc = cxl_msg.set_double(38, self.qty)
        if rc <= 0: raise FIXMessageError(38, cxl_msg.get_last_error_str(rc))

        # Tag 41: OrigClOrdID
        cdef char* orig_clord = self.msg.get_str(11)
        if orig_clord == NULL:
            raise FIXMessageError(41, self.msg.get_last_error_str(self.msg.get_last_error()))
        rc = cxl_msg.set_str(41, orig_clord, clord_len)
        if rc <= 0:
            if rc == -20:
                # ERR_UNEXPECTED_TYPE_SIZE
                # Highly likely len(orig_clord) != len(req_clord_id)
                #  This is unexpected behaviour, all ClOrdIDs must be with the same len
                raise FIXMessageError(41, b'Possible len(orig_clord) != len(req_clord_id), all ClOrdID must have the same length')
            else:
                raise FIXMessageError(41, cxl_msg.get_last_error_str(rc))

        # Tag 54: Side
        cdef char* side = self.msg.get_char(54)
        if side == NULL:
            raise FIXMessageError(54, self.msg.get_last_error_str(self.msg.get_last_error()))
        cxl_msg.set_char(54, side[0])

        # Tag 60: Transact time
        rc = cxl_msg.set_utc_timestamp(60, datetime_nsnow())
        if rc <= 0:
            raise FIXMessageError(60, cxl_msg.get_last_error_str(rc))

        # TODO: set instrument info
        # Tag 55: Symbol set to v2 symbol? -- it's going to be stable
        # Tag 48: SecurityID - set to instrument_info (uint64_t instrument_id) -- it's going to be stable
        # Tag 22: SecurityIDSource - quote cache ticker index - may be dynamic and actual during short time (not reliable but fast!)

        return cxl_msg
