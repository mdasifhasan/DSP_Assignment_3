from Chord import ChordNode
import sys
import zmq
import time
from threading import Thread


class EventService:
    def __init__(self, ip, start_mode="create", join_ip=""):
        print "Running Event Service"
        self.chord = ChordNode(ip, start_mode, join_ip)
        self.ip = ip
        t = Thread(target=self.broker)
        t.start()


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


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        ip = "127.0.0.1"
    if len(sys.argv) >= 3:
        start_mode = "join"
        join_ip = sys.argv[2]
        EventService(ip, start_mode, join_ip)
    else:
        EventService(ip)