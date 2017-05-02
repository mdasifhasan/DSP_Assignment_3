import zmq
import time
from hash_ring import HashRing
from random import randint
import sys

class Publisher:
    def __init__(self, join_ip, topic = "topic"):

        self.listID = []
        for i in range(0, 256):
            self.listID.append(i)
        ring = HashRing(self.listID)
        self.id = ring.get_node(topic)
        print "id assigned:", self.id
        self.join_ip = join_ip
        self.topic = topic

        self.context = zmq.Context()
        s, self.event_service_ip = self.call_remote_procedure(self.join_ip, "findSuccessor", str(self.id)).split()
        print "EventService ip: ", self.event_service_ip
        self.event_service_id = int(s)

        xsub_url = "tcp://" + self.event_service_ip + ":5556"

        socket = self.context.socket(zmq.PUB)
        socket.connect(xsub_url)
        # pub.bind(xpub_url)
        for n in range(1000):
            priority = randint(0, 9)
            msg = [topic, str(priority), str(n)]
            socket.send_multipart(msg)
            print "for the message %s with priority %d" % (
            topic, priority), "process time in publisher is", time.clock()

            time.sleep(1.25)

    def call_remote_procedure(self, ip, proc, data):
        socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + ip + ":5550"
        socket.connect(connect_str)
        socket.send("%s %s" % (proc, data))
        return socket.recv()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        ip = "127.0.0.1"
    if len(sys.argv) >= 3:
        topic = sys.argv[2]
    else:
        topic = "topic"

    Publisher(ip, topic)