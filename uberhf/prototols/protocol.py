import zmq
import zmq.asyncio


class Protocol:
    def __init__(self, context: zmq.asyncio.Context, is_server: bool, reqrep_url: str = None, pubsub_url: str = None):
        self.reqrep_socket = None
        self.pubsub_socket = None
        self.is_server = is_server

        if reqrep_url:
            if is_server:
                self.reqrep_socket = context.socket(zmq.REP)
                self.reqrep_socket.bind(reqrep_url)
            else:
                self.reqrep_socket = context.socket(zmq.REQ)
                self.reqrep_socket.connect(reqrep_url)

    async def req(self, message):
        assert not self.is_server, f'Not allowed on servers'
        assert self.reqrep_socket, f'Not initialized'

        print(f'Client req >>: {message}')
        await self.reqrep_socket.send(message)
        reply = await self.reqrep_socket.recv()

        print(f'Client req reply <<: {reply}')
        return reply

    async def rep(self, req_message):
        assert self.is_server, f'Not allowed on clients'
        assert self.reqrep_socket, f'Not initialized'
        print(f'Server rep <<: {req_message}')
        await self.reqrep_socket.send(b'response: ' + req_message)

    def close(self):
        if self.reqrep_socket:
            self.reqrep_socket.close(0)