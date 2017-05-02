import zmq
import time
from threading import Thread

class Net:
    def __init__(self, ip, callbackMsgRcvd):
        self.callback = callbackMsgRcvd
        self.context = zmq.Context()
        self.ip = ip
        t = Thread(target=self.start_server)
        t.start()

    def publish(self, ip, topic, msg):
        pass

    def subscribe(self, ip, topic):
        pass

    def call_remote_procedure(self, ip, proc, data):
        socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + ip + ":5550"
        socket.connect(connect_str)
        socket.send("%s %s" % (proc, data))
        #print "sent to remote node"
        return socket.recv()

    def start_server(self):
        socket = self.context.socket(zmq.REP)
        #socket.bind("tcp://*:5555")
        socket.bind("tcp://" + self.ip + ":5550")
        #print "Starting server:", self.ip
        while True:
            #  Wait for next request from client
            message = socket.recv()
            #print("Server: Received request: %s" % message)
            ret_msg = self.callback(message)
            #print "Server: Responding to message:", ret_msg
            socket.send(ret_msg)

