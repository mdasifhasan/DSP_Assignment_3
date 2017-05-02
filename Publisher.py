import zmq
import time
from random import randint
import sys
from kazoo.client import KazooClient

class Publisher:
    def __init__(self, join_ip, topic = "topic"):
        self.leader_ip = None
        self.topic = topic
        self.context = zmq.Context()
        self.socket = None
        self.zoo_address = self.register(join_ip)
        print "Zoo address: ", self.zoo_address

        self.zoo = KazooClient(self.zoo_address)
        self.zoo.start()

        self.leader_changed = False

        self.zoo.get("leader_info", watch=self.watch_leader)


        if self.zoo.exists("leader_info") != None:
            leader = self.zoo.get("leader_info")
            self.event_service_ip, stat = leader
            print "Leader IP: ", self.event_service_ip
            self.leader_changed = True
            if self.socket != None:
                self.socket.close()
            self.publish()


    def watch_leader(self, event):
        if self.zoo.exists("leader_info") != None:
            leader = self.zoo.get("leader_info")
            self.event_service_ip, stat = leader
            print "Leader IP: ", self.event_service_ip
            self.zoo.get("leader_info", watch=self.watch_leader)
            self.leader_changed = True
            if self.socket != None:
                self.socket.close()
            self.publish()



    def publish(self):
        xsub_url = "tcp://" + self.event_service_ip + ":5556"
        print "ES is changed to:" + xsub_url
        socket = self.context.socket(zmq.PUB)
        socket.connect(xsub_url)
        self.leader_changed = False
        # pub.bind(xpub_url)
        for n in range(1000):
            try:
                if self.leader_changed:
                    break
                priority = randint(0, 9)
                msg = [topic, str(priority), str(n)]
                socket.send_multipart(msg)
                print "for the message %s with priority %d" % (
                topic, priority), "process time in publisher is", time.clock()

                time.sleep(1.25)
            except:
                break

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