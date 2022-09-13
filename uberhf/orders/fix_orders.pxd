from .fix_msg cimport FIXMsgStruct, FIXMsg
from uberhf.datafeed.quote_info import QuoteInfo

from libc.stdint cimport uint64_t
from uberhf.datafeed.quotes_cache cimport QCRecord

cdef extern from *:
    """
    // FIX Order Status 39
    #define FIX_OS_CREA        'Z'      // Non-FIX type, only for internal use!
    #define FIX_OS_NEW         '0'
    #define FIX_OS_PFILL       '1'
    #define FIX_OS_FILL        '2'
    #define FIX_OS_DFD         '3'
    #define FIX_OS_CXL         '4'
    #define FIX_OS_PCXL        '6'
    #define FIX_OS_STP         '7'
    #define FIX_OS_REJ         '6'
    #define FIX_OS_SUSP        '9'
    #define FIX_OS_PNEW        'A'
    #define FIX_OS_CALC        'B'
    #define FIX_OS_EXP         'C'
    #define FIX_OS_ACCPT       'D'    
    #define FIX_OS_PREP        'E'

    // FIX Execution report 150
    #define FIX_ET_NEW         '0'
    #define FIX_ET_DFD         '3'
    #define FIX_ET_CXL         '4'
    #define FIX_ET_REP         '5'
    #define FIX_ET_PCXL        '6'
    #define FIX_ET_STP         '7'
    #define FIX_ET_REJ         '8'
    #define FIX_ET_SUSP        '9'
    #define FIX_ET_PNEW        'A'
    #define FIX_ET_CALC        'B'
    #define FIX_ET_EXP         'C'
    #define FIX_ET_PREP        'E'
    #define FIX_ET_TRADE       'F'
    #define FIX_ET_STATUS      'I'
    """
    const char FIX_OS_CREA        #Z#      // Non-FIX type, only for internal use!
    const char FIX_OS_NEW         #0#
    const char FIX_OS_PFILL       #1#
    const char FIX_OS_FILL        #2#
    const char FIX_OS_DFD         #3#
    const char FIX_OS_CXL         #4#
    const char FIX_OS_PCXL        #6#
    const char FIX_OS_STP         #7#
    const char FIX_OS_REJ         #6#
    const char FIX_OS_SUSP        #9#
    const char FIX_OS_PNEW        #A#
    const char FIX_OS_CALC        #B#
    const char FIX_OS_EXP         #C#
    const char FIX_OS_ACCPT       #D#
    const char FIX_OS_PREP        #E#
    
    const char FIX_ET_NEW         #0#
    const char FIX_ET_DFD         #3#
    const char FIX_ET_CXL         #4#
    const char FIX_ET_REP         #5#
    const char FIX_ET_PCXL        #6#
    const char FIX_ET_STP         #7#
    const char FIX_ET_REJ         #8#
    const char FIX_ET_SUSP        #9#
    const char FIX_ET_PNEW        #A#
    const char FIX_ET_CALC        #B#
    const char FIX_ET_EXP         #C#
    const char FIX_ET_PREP        #E#
    const char FIX_ET_TRADE       #F#
    const char FIX_ET_STATUS      #I#



cdef class FIXNewOrderSingle:
    cdef FIXMsgStruct * msg
    cdef QCRecord * q
    cdef uint64_t clord_id
    cdef uint64_t orig_clord_id
    cdef double price
    cdef double target_price
    cdef int side
    cdef double qty
    cdef double cum_qty
    cdef double leaves_qty
    cdef char status


    @staticmethod
    cdef FIXNewOrderSingle create(QCRecord * q,
                                  int account_id,
                                  double price,
                                  double qty,
                                  double target_price = *,
                                  char order_type = *,
                                  char time_in_force = *,
                                  )

    cdef int register(self, uint64_t clord_id, uint64_t orig_clord_id)

    cdef int process_execution_report(self, FIXMsgStruct * m)

    cdef FIXMsgStruct * cancel_req(self)




