from uberhf.prototols.protocol import Protocol
import time
import zmq
import zmq.asyncio
import asyncio
from uberhf.datafeed.mem_pool_quotes import QRec
ctx = zmq.asyncio.Context()

if __name__ == '__main__':
    import zmq

    # Prepare our context and sockets

    server = Protocol(ctx, is_server=True, reqrep_url='tcp://*:61000')
    server1 = Protocol(ctx, is_server=True, reqrep_url='tcp://*:61001')
    client = Protocol(ctx, is_server=False, reqrep_url='tcp://localhost:61000')

    async def server_routine():
        poller = zmq.asyncio.Poller()
        poller.register(server.reqrep_socket, zmq.POLLIN)
        poller.register(server1.reqrep_socket, zmq.POLLIN)

        while True:
            try:
                socks = await poller.poll()
                print(socks)

                socks = dict(socks)
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break

            if server.reqrep_socket and server.reqrep_socket in socks:
                req_message = await server.reqrep_socket.recv()
                await server.rep(req_message)

    async def client_routine():
        while True:
            try:
                await client.req(b'ping')
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break

    loop = asyncio.get_event_loop()

    loop.run_until_complete(asyncio.gather(
            client_routine(),
            server_routine(),
    ))

    server.close()
    client.close()

