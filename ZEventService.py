from zoo import *
import sys
import pprint
import zmq
from threading import Thread
import time

class ZKeventService:
    def __init__(self, self_ip = None, zoo_ip = None, listener = None):
        self.ip = self_ip
        self.zoo_address = '10.0.0.1:2181'

    	pprint.pprint("starting event service")
    	self.kazoo = Kazoo(zoo_ip, self_ip, self.im_the_leader_listener)

        self.server_thread = Thread(target=self.start_server)
        self.server_thread.start()

        pprint.pprint("event service started")

        self.quit_server = False

        self.ip = self_ip
        t = Thread(target=self.broker)
        t.start()


    def stop(self):
     	self.kazoo.stop()
        self.quit_server = True
        self.socket.close()
        self.context.term()
        pprint.pprint("event service stopped")

    def im_the_leader_listener(self):
        print "EventService: I am the new leader...!!"


    def start_server(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        #socket.bind("tcp://*:5555")
        self.socket.bind("tcp://" + self.ip + ":5550")
        print "Server started:", self.ip + ":5550"
        while True:
            try:
                #  Wait for next request from client
                message = self.socket.recv()
                print("Server: Received request: %s" % message)
                ret_msg = self.server_callback(message)
                print "Server: Responding to message:", ret_msg
                self.socket.send(ret_msg)
            except:
                if self.quit_server:
                    break
        pprint.pprint("server stopped")
    def server_callback(self, msg):
        topic = msg
        if topic == "register":
            return self.zoo_address


    def broker(self):
        print "Starting thread for Broker"
        xpub_url = "tcp://" + self.ip + ":5555"
        xsub_url = "tcp://" + self.ip + ":5556"
        list1 = []
        list2 = []
        ctx = zmq.Context()
        xpub = ctx.socket(zmq.XPUB)
        xpub.bind(xpub_url)
        xsub = ctx.socket(zmq.XSUB)
        xsub.bind(xsub_url)

        poller = zmq.Poller()
        poller.register(xpub, zmq.POLLIN)
        poller.register(xsub, zmq.POLLIN)

        while True:
            events = dict(poller.poll(1000))
            print len(events)
            if xpub in events:
                print "xpub in events"
                message = xpub.recv_multipart()
                list2.append(message)
                if message[0] == b'\x01':
                    topic = message[1:]
                    if topic in list2:
                        print "Sending cached topic %s", topic
                        xpub.send_multipart(topic)
                # for n in range(30):
                list1.append(message)
                list1.sort()

                print "[BROKER] subscription message: %r" % message[0]

                for x in list1:
                    xsub.send_multipart(x)
                    print "1 process time of", x, "in intermediary is", time.clock()

            if xsub in events:
                print "xsub in events"
                message = xsub.recv_multipart()
                list2.append(message)

                # for n in range(30):
                list1.append(message)

                list1.sort()

                for x in list1:
                    xpub.send_multipart(x)
                    print "2 process time of", x, "in intermediary is", time.clock()

self_ip = None
zoo_ip = None
if len(sys.argv) <= 1:
	pprint.pprint("need params: self_ip zoo_ip")
	sys.exit(0)

if len(sys.argv) > 1:
	self_ip = sys.argv[1]
if len(sys.argv) > 2:
	zoo_ip = sys.argv[2]

ES = ZKeventService(self_ip, zoo_ip)


while raw_input() != "q":
    sleep(0.1)
    continue

ES.stop()