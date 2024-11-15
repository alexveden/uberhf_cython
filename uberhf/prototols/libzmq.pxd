
cdef extern from "zmq.h" nogil:
    ctypedef int fd_t "ZMQ_FD_T"

    enum: errno
    const char *zmq_strerror(int errnum)
    int zmq_errno()

    void *zmq_ctx_new()
    int zmq_ctx_destroy(void *context)
    int zmq_ctx_set(void *context, int option, int optval)
    int zmq_ctx_get(void *context, int option)
    void *zmq_init(int io_threads)
    int zmq_term(void *context)

    ctypedef void * zmq_msg_t "zmq_msg_t"

    ctypedef void zmq_free_fn(void *data, void *hint)

    int zmq_msg_init(zmq_msg_t *msg)
    int zmq_msg_init_size(zmq_msg_t *msg, size_t size)
    int zmq_msg_init_data(zmq_msg_t *msg, void *data,
                          size_t size, zmq_free_fn *ffn, void *hint)
    int zmq_msg_send(zmq_msg_t *msg, void *s, int flags)
    int zmq_msg_recv(zmq_msg_t *msg, void *s, int flags)
    int zmq_msg_close(zmq_msg_t *msg)
    int zmq_msg_move(zmq_msg_t *dest, zmq_msg_t *src)
    int zmq_msg_copy(zmq_msg_t *dest, zmq_msg_t *src)
    void *zmq_msg_data(zmq_msg_t *msg)
    size_t zmq_msg_size(zmq_msg_t *msg)
    int zmq_msg_more(zmq_msg_t *msg)
    int zmq_msg_get(zmq_msg_t *msg, int option)
    int zmq_msg_set(zmq_msg_t *msg, int option, int optval)
    const char *zmq_msg_gets(zmq_msg_t *msg, const char *property)
    int zmq_has(const char *capability)

    void *zmq_socket(void *context, int type)
    int zmq_close(void *s)
    int zmq_setsockopt(void *s, int option, void *optval, size_t optvallen)
    int zmq_getsockopt(void *s, int option, void *optval, size_t *optvallen)
    int zmq_bind(void *s, char *addr)
    int zmq_connect(void *s, char *addr)
    int zmq_unbind(void *s, char *addr)
    int zmq_disconnect(void *s, char *addr)

    int zmq_socket_monitor(void *s, char *addr, int flags)

    # send/recv
    int zmq_send(void *s, const void *buf, size_t n, int flags)
    int zmq_recv(void *s, void *buf, size_t n, int flags)

    ctypedef struct zmq_pollitem_t:
        void *socket
        fd_t fd
        short events
        short revents

    int zmq_poll(zmq_pollitem_t *items, int nitems, long timeout)

    int zmq_device(int device_, void *insocket_, void *outsocket_)
    int zmq_proxy(void *frontend, void *backend, void *capture)
    int zmq_proxy_steerable(void *frontend,
                            void *backend,
                            void *capture,
                            void *control)

    int zmq_curve_keypair(char *z85_public_key, char *z85_secret_key)
    int zmq_curve_public(char *z85_public_key, char *z85_secret_key)

    # 4.2 draft
    int zmq_join(void *s, const char *group)
    int zmq_leave(void *s, const char *group)

    int zmq_msg_set_group(zmq_msg_t *msg, const char *group)
    const char *zmq_msg_group(zmq_msg_t *msg)


    # LIB ZMQ Enums
    enum: PYZMQ_DRAFT_API
    enum: ZMQ_VERSION
    enum: ZMQ_VERSION_MAJOR
    enum: ZMQ_VERSION_MINOR
    enum: ZMQ_VERSION_PATCH
    enum: ZMQ_IO_THREADS
    enum: ZMQ_MAX_SOCKETS
    enum: ZMQ_SOCKET_LIMIT
    enum: ZMQ_THREAD_PRIORITY
    enum: ZMQ_THREAD_SCHED_POLICY
    enum: ZMQ_MAX_MSGSZ
    enum: ZMQ_MSG_T_SIZE
    enum: ZMQ_THREAD_AFFINITY_CPU_ADD
    enum: ZMQ_THREAD_AFFINITY_CPU_REMOVE
    enum: ZMQ_THREAD_NAME_PREFIX
    enum: ZMQ_STREAMER
    enum: ZMQ_FORWARDER
    enum: ZMQ_QUEUE
    enum: ZMQ_EAGAIN "EAGAIN"
    enum: ZMQ_EFAULT "EFAULT"
    enum: ZMQ_EINVAL "EINVAL"
    enum: ZMQ_ENOTSUP "ENOTSUP"
    enum: ZMQ_EPROTONOSUPPORT "EPROTONOSUPPORT"
    enum: ZMQ_ENOBUFS "ENOBUFS"
    enum: ZMQ_ENETDOWN "ENETDOWN"
    enum: ZMQ_EADDRINUSE "EADDRINUSE"
    enum: ZMQ_EADDRNOTAVAIL "EADDRNOTAVAIL"
    enum: ZMQ_ECONNREFUSED "ECONNREFUSED"
    enum: ZMQ_EINPROGRESS "EINPROGRESS"
    enum: ZMQ_ENOTSOCK "ENOTSOCK"
    enum: ZMQ_EMSGSIZE "EMSGSIZE"
    enum: ZMQ_EAFNOSUPPORT "EAFNOSUPPORT"
    enum: ZMQ_ENETUNREACH "ENETUNREACH"
    enum: ZMQ_ECONNABORTED "ECONNABORTED"
    enum: ZMQ_ECONNRESET "ECONNRESET"
    enum: ZMQ_ENOTCONN "ENOTCONN"
    enum: ZMQ_ETIMEDOUT "ETIMEDOUT"
    enum: ZMQ_EHOSTUNREACH "EHOSTUNREACH"
    enum: ZMQ_ENETRESET "ENETRESET"
    enum: ZMQ_EFSM "EFSM"
    enum: ZMQ_ENOCOMPATPROTO "ENOCOMPATPROTO"
    enum: ZMQ_ETERM "ETERM"
    enum: ZMQ_EMTHREAD "EMTHREAD"
    enum: ZMQ_EVENT_CONNECTED
    enum: ZMQ_EVENT_CONNECT_DELAYED
    enum: ZMQ_EVENT_CONNECT_RETRIED
    enum: ZMQ_EVENT_LISTENING
    enum: ZMQ_EVENT_BIND_FAILED
    enum: ZMQ_EVENT_ACCEPTED
    enum: ZMQ_EVENT_ACCEPT_FAILED
    enum: ZMQ_EVENT_CLOSED
    enum: ZMQ_EVENT_CLOSE_FAILED
    enum: ZMQ_EVENT_DISCONNECTED
    enum: ZMQ_EVENT_MONITOR_STOPPED
    enum: ZMQ_EVENT_ALL
    enum: ZMQ_HANDSHAKE_FAILED_NO_DETAIL
    enum: ZMQ_HANDSHAKE_SUCCEEDED
    enum: ZMQ_HANDSHAKE_FAILED_PROTOCOL
    enum: ZMQ_HANDSHAKE_FAILED_AUTH
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_UNSPECIFIED
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_UNEXPECTED_COMMAND
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_INVALID_SEQUENCE
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_KEY_EXCHANGE
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_UNSPECIFIED
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_MESSAGE
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_HELLO
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_INITIATE
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_ERROR
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_READY
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MALFORMED_COMMAND_WELCOME
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_INVALID_METADATA
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_CRYPTOGRAPHIC
    enum: ZMQ_PROTOCOL_ERROR_ZMTP_MECHANISM_MISMATCH
    enum: ZMQ_PROTOCOL_ERROR_ZAP_UNSPECIFIED
    enum: ZMQ_PROTOCOL_ERROR_ZAP_MALFORMED_REPLY
    enum: ZMQ_PROTOCOL_ERROR_ZAP_BAD_REQUEST_ID
    enum: ZMQ_PROTOCOL_ERROR_ZAP_BAD_VERSION
    enum: ZMQ_PROTOCOL_ERROR_ZAP_INVALID_STATUS_CODE
    enum: ZMQ_PROTOCOL_ERROR_ZAP_INVALID_METADATA
    enum: ZMQ_PROTOCOL_ERROR_WS_UNSPECIFIED
    enum: ZMQ_EVENT_PIPES_STATS
    enum: ZMQ_EVENT_ALL_V1
    enum: ZMQ_EVENT_ALL_V2
    enum: ZMQ_DONTWAIT
    enum: ZMQ_SNDMORE
    enum: ZMQ_NOBLOCK
    enum: ZMQ_MORE
    enum: ZMQ_SHARED
    enum: ZMQ_SRCFD
    enum: ZMQ_POLLIN
    enum: ZMQ_POLLOUT
    enum: ZMQ_POLLERR
    enum: ZMQ_POLLPRI
    enum: ZMQ_NULL
    enum: ZMQ_PLAIN
    enum: ZMQ_CURVE
    enum: ZMQ_GSSAPI
    enum: ZMQ_HWM
    enum: ZMQ_AFFINITY
    enum: ZMQ_ROUTING_ID
    enum: ZMQ_SUBSCRIBE
    enum: ZMQ_UNSUBSCRIBE
    enum: ZMQ_RATE
    enum: ZMQ_RECOVERY_IVL
    enum: ZMQ_SNDBUF
    enum: ZMQ_RCVBUF
    enum: ZMQ_RCVMORE
    enum: ZMQ_FD
    enum: ZMQ_EVENTS
    enum: ZMQ_TYPE
    enum: ZMQ_LINGER
    enum: ZMQ_RECONNECT_IVL
    enum: ZMQ_BACKLOG
    enum: ZMQ_RECONNECT_IVL_MAX
    enum: ZMQ_MAXMSGSIZE
    enum: ZMQ_SNDHWM
    enum: ZMQ_RCVHWM
    enum: ZMQ_MULTICAST_HOPS
    enum: ZMQ_RCVTIMEO
    enum: ZMQ_SNDTIMEO
    enum: ZMQ_LAST_ENDPOINT
    enum: ZMQ_ROUTER_MANDATORY
    enum: ZMQ_TCP_KEEPALIVE
    enum: ZMQ_TCP_KEEPALIVE_CNT
    enum: ZMQ_TCP_KEEPALIVE_IDLE
    enum: ZMQ_TCP_KEEPALIVE_INTVL
    enum: ZMQ_IMMEDIATE
    enum: ZMQ_XPUB_VERBOSE
    enum: ZMQ_ROUTER_RAW
    enum: ZMQ_IPV6
    enum: ZMQ_MECHANISM
    enum: ZMQ_PLAIN_SERVER
    enum: ZMQ_PLAIN_USERNAME
    enum: ZMQ_PLAIN_PASSWORD
    enum: ZMQ_CURVE_SERVER
    enum: ZMQ_CURVE_PUBLICKEY
    enum: ZMQ_CURVE_SECRETKEY
    enum: ZMQ_CURVE_SERVERKEY
    enum: ZMQ_PROBE_ROUTER
    enum: ZMQ_REQ_CORRELATE
    enum: ZMQ_REQ_RELAXED
    enum: ZMQ_CONFLATE
    enum: ZMQ_ZAP_DOMAIN
    enum: ZMQ_ROUTER_HANDOVER
    enum: ZMQ_TOS
    enum: ZMQ_CONNECT_ROUTING_ID
    enum: ZMQ_GSSAPI_SERVER
    enum: ZMQ_GSSAPI_PRINCIPAL
    enum: ZMQ_GSSAPI_SERVICE_PRINCIPAL
    enum: ZMQ_GSSAPI_PLAINTEXT
    enum: ZMQ_HANDSHAKE_IVL
    enum: ZMQ_SOCKS_PROXY
    enum: ZMQ_XPUB_NODROP
    enum: ZMQ_BLOCKY
    enum: ZMQ_XPUB_MANUAL
    enum: ZMQ_XPUB_WELCOME_MSG
    enum: ZMQ_STREAM_NOTIFY
    enum: ZMQ_INVERT_MATCHING
    enum: ZMQ_HEARTBEAT_IVL
    enum: ZMQ_HEARTBEAT_TTL
    enum: ZMQ_HEARTBEAT_TIMEOUT
    enum: ZMQ_XPUB_VERBOSER
    enum: ZMQ_CONNECT_TIMEOUT
    enum: ZMQ_TCP_MAXRT
    enum: ZMQ_THREAD_SAFE
    enum: ZMQ_MULTICAST_MAXTPDU
    enum: ZMQ_VMCI_BUFFER_SIZE
    enum: ZMQ_VMCI_BUFFER_MIN_SIZE
    enum: ZMQ_VMCI_BUFFER_MAX_SIZE
    enum: ZMQ_VMCI_CONNECT_TIMEOUT
    enum: ZMQ_USE_FD
    enum: ZMQ_GSSAPI_PRINCIPAL_NAMETYPE
    enum: ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE
    enum: ZMQ_BINDTODEVICE
    enum: ZMQ_IDENTITY
    enum: ZMQ_CONNECT_RID
    enum: ZMQ_TCP_ACCEPT_FILTER
    enum: ZMQ_IPC_FILTER_PID
    enum: ZMQ_IPC_FILTER_UID
    enum: ZMQ_IPC_FILTER_GID
    enum: ZMQ_IPV4ONLY
    enum: ZMQ_DELAY_ATTACH_ON_CONNECT
    enum: ZMQ_FAIL_UNROUTABLE
    enum: ZMQ_ROUTER_BEHAVIOR
    enum: ZMQ_ZAP_ENFORCE_DOMAIN
    enum: ZMQ_LOOPBACK_FASTPATH
    enum: ZMQ_METADATA
    enum: ZMQ_MULTICAST_LOOP
    enum: ZMQ_ROUTER_NOTIFY
    enum: ZMQ_XPUB_MANUAL_LAST_VALUE
    enum: ZMQ_SOCKS_USERNAME
    enum: ZMQ_SOCKS_PASSWORD
    enum: ZMQ_IN_BATCH_SIZE
    enum: ZMQ_OUT_BATCH_SIZE
    enum: ZMQ_WSS_KEY_PEM
    enum: ZMQ_WSS_CERT_PEM
    enum: ZMQ_WSS_TRUST_PEM
    enum: ZMQ_WSS_HOSTNAME
    enum: ZMQ_WSS_TRUST_SYSTEM
    enum: ZMQ_ONLY_FIRST_SUBSCRIBE
    enum: ZMQ_RECONNECT_STOP
    enum: ZMQ_HELLO_MSG
    enum: ZMQ_DISCONNECT_MSG
    enum: ZMQ_PRIORITY
    enum: ZMQ_PAIR
    enum: ZMQ_PUB
    enum: ZMQ_SUB
    enum: ZMQ_REQ
    enum: ZMQ_REP
    enum: ZMQ_DEALER
    enum: ZMQ_ROUTER
    enum: ZMQ_PULL
    enum: ZMQ_PUSH
    enum: ZMQ_XPUB
    enum: ZMQ_XSUB
    enum: ZMQ_STREAM
    enum: ZMQ_XREQ
    enum: ZMQ_XREP
    enum: ZMQ_SERVER
    enum: ZMQ_CLIENT
    enum: ZMQ_RADIO
    enum: ZMQ_DISH
    enum: ZMQ_GATHER
    enum: ZMQ_SCATTER
    enum: ZMQ_DGRAM
    enum: ZMQ_PEER
    enum: ZMQ_CHANNEL