import sys
import zmq
port = "5556"
import time
import pickle

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from server...")
socket.connect("tcp://localhost:%s" % port)

topicfilter = b""
socket.setsockopt(zmq.SUBSCRIBE, topicfilter)

# Process 5 updates
total_value = 0
t_begin = time.time()
n_messages = 100000
for update_nbr in range(n_messages):
    string = socket.recv()
    msg = pickle.loads(string)

t_end = time.time()
duration = t_end-t_begin

print(f'{n_messages} received in {duration}s: Throughput {n_messages/duration:0.0f} msg/sec')

