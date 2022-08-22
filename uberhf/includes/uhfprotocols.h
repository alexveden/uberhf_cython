//
// GLOBAL MODULES ID (typically it's used only in lifetime number generation for a clarity sake)
// MUST be between 1 and 40
#define MODULE_ID_TEST 40
#define MODULE_ID_UHFEED 10
#define MODULE_ID_ORDER_ROUTER 20



#define TRANSPORT_HDR_MGC 29517
#define TRANSPORT_SENDER_SIZE 5

//
// Transport error codes
//
#define TRANSPORT_ERR_OK 0
#define TRANSPORT_ERR_ZMQ -64000
#define TRANSPORT_ERR_BAD_SIZE -64001
#define TRANSPORT_ERR_BAD_HEADER -64002
#define TRANSPORT_ERR_BAD_PARTSCOUNT -64003
#define TRANSPORT_ERR_SOCKET_CLOSED -64004
#define TRANSPORT_ERR_NULL_DATA -64005
#define TRANSPORT_ERR_NULL_DEALERID -64006

//
// Unique protocol IDs
//
#define PROTOCOL_ID_NONE '\0'
#define PROTOCOL_ID_BASE 'B'
#define PROTOCOL_ID_TEST 'T'
#define PROTOCOL_ID_DATASOURCE 'S'

#define PROTOCOL_ERR_GENERIC      -55000
#define PROTOCOL_ERR_SIZE         -55001
#define PROTOCOL_ERR_WRONG_TYPE   -55002
#define PROTOCOL_ERR_WRONG_ORDER  -55003

