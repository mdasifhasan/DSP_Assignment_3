import zmq
import time
from random import randint
import sys

class Publisher:
    def __init__(self, join_ip, topic = "topic"):
        self.topic = topic
        self.context = zmq.Context()
        self.event_service_ip = self.register(join_ip)
        print "EventService ip: ", self.event_service_ip


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

    def register(self, ip):
        socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + ip + ":5550"
        socket.connect(connect_str)
        socket.send("register")
        return socket.recv()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        print "usage:python Publisher.py join_ip topic"
        sys.exit(0)
    if len(sys.argv) >= 3:
        topic = sys.argv[2]
    else:
        topic = "topic"

    Publisher(ip, topic)