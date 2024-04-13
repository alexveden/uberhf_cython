import zmq
import random
import sys
import time
import pickle
port = "5556"

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind(f"tcp://*:{port}")

while True:
    #topic = random.randrange(9999,10005)
    #messagedata = random.randrange(1,215) - 80
    #print("%d %d" % (topic, messagedata))
    #socket.send("%d %d" % (topic, messagedata))

    socket.send(pickle.dumps({'e': 'test', 'cid': 'ok'}), copy=False)
    #time.sleep(1)