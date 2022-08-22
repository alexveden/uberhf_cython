from uberhf.prototols.transport cimport Transport, TransportHeader
from uberhf.prototols.libzmq cimport *
from zmq.backend.cython.context cimport Context
from libc.stdint cimport uint64_t
from libc.string cimport strcpy, memset, memcpy
from libc.stdio cimport printf
#from zmq import Context
import time

ctypedef struct SomeMessage:
    TransportHeader header

    int data

cpdef main():
    cdef void * ctx = zmq_ctx_new()
    transport = Transport(<uint64_t>ctx, b'tcp://localhost:7100', ZMQ_DEALER, b'CLI')

    #ctx = Context()
    #transport = Transport(<uint64_t>ctx.underlying, b'tcp://*:7100', ZMQ_REP)
    cdef int n_sent
    cdef void * data
    cdef size_t data_size
    cdef char buf[300]

    cdef int n_messages = 100000
    print(f'Sending {n_messages}')
    t_begin = time.time()

    cdef SomeMessage msg
    msg.data = 123

    for i in range(n_messages):
        n_sent = transport.send(NULL, &msg, sizeof(SomeMessage), no_copy=False)
        #printf('Sent: hi, %d bytes\n', n_sent)

        #data = transport.receive(&data_size)
        #printf('Reply: %d bytes\n', data_size)
        #transport.receive_finalize(data)
    t_end = time.time()

    print(f'#{transport.msg_sent} sent / #{transport.msg_errors} errs in {t_end-t_begin}seconds, {transport.msg_sent/(t_end-t_begin)} msg/sec')

    transport.close()
    print('Closing context')

    zmq_ctx_destroy(ctx)
    #ctx.term()
    print('Done')



