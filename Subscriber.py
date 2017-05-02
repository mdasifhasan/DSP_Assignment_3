import zmq
import time
from threading import Thread
from random import randint
import sys


class Subscriber:
    def __init__(self, join_ip, topic = "topic"):
        self.topic = topic
        self.context = zmq.Context()
        self.event_service_ip = self.register(join_ip)
        print "EventService ip: ", self.event_service_ip
        self.subscriber()

    def register(self, ip):
        socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + ip + ":5550"
        socket.connect(connect_str)
        socket.send("register")
        return socket.recv()

    def subscriber(self):
        xpub_url = "tcp://" + self.event_service_ip + ":5555"
        count = 1
        list1 = []
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(xpub_url)
        topics = []
        topics.append(self.topic)
        subscription = set()
        while True:
            r = randint(0, len(topics))
            if r < len(topics):
                topic = topics[r]
                if topic not in subscription:
                    subscription.add(topic)
                    sub.setsockopt(zmq.SUBSCRIBE, topic)
            r2 = randint(0, len(topics))
            if r2 != r and r2 < len(topics):
                topic = topics[r2]
                if topic in subscription:
                    subscription.remove(topic)
                    sub.setsockopt(zmq.UNSUBSCRIBE, topic)
            time.sleep(0.3)

            while True:
                if sub.poll(timeout=0):
                    for n in range(20):
                        list1.append(sub.recv_multipart())

                    list1.sort()
                    for x in list1:
                        print "received", x
                        print "for the message %s with priority %s" % (
                        x[0], x[1]), "process time in subscriber is", time.clock()
                else:
                    break


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        print "usage:python Subscriber.py join_ip topic"
        exit(0)
    if len(sys.argv) >= 3:
        topic = sys.argv[2]
    else:
        topic = "topic"

    Subscriber(ip, topic)