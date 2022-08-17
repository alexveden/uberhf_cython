from uberhf.prototols.transport cimport Transport
from uberhf.prototols.libzmq cimport *
from zmq.backend.cython.context cimport Context
from libc.stdint cimport uint64_t
from libc.string cimport strcpy, memset, memcpy
from libc.stdio cimport printf
#from zmq import Context
import  time

cdef extern from "assert.h":
    # Replacing name to avoid conflict with python assert keyword!
    void cassert "assert"(bint)

cpdef main():
    cdef void * ctx = zmq_ctx_new()
    transport = Transport(<uint64_t>ctx, b'tcp://*:7100', ZMQ_ROUTER, b'SRV')

    #ctx = Context()
    #transport = Transport(<uint64_t>ctx.underlying, b'tcp://*:7100', ZMQ_REP)
    cdef void * data
    cdef int n_sent
    cdef size_t data_size
    cdef char buf[300]

    print('Listening events')
    cdef int i = 0

    while True:
        data = transport.receive(&data_size)
        if i == 0:
            t_begin = time.time()

        cassert(data != NULL)

        # Do some work here
        #memset(buf, 0, 300)
        #memcpy(buf, data, min(data_size, 300))

        #printf('Received: %d bytes\n', data_size)
        transport.receive_finalize(data)

        #n_sent = transport.send( b'hello', 4, no_copy=False)
        #printf('REP: %d\n', n_sent)
        i += 1

        if i == 100000:
            break


    t_end = time.time()

    print(f'#{transport.msg_received} received / #{transport.msg_errors} erorrs in {t_end-t_begin}seconds, {i/(t_end-t_begin)} msg/sec')


    transport.close()
    print('Closing context')

    zmq_ctx_destroy(ctx)
    #ctx.term()
    print('Done')



