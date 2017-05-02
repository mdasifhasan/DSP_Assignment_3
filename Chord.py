from hash_ring import HashRing
from Net import Net
import sys
from threading import Thread
import time
class Finger:
    def __init__(self):
        self.succssor = 0
        self.ip = ""



class ChordNode:
    def __init__(self, ip, start_mode="create", join_ip=""):
        self.ip = ip
        self.listID = []
        for i in range(0, 256):
            self.listID.append(i)
        self.id = int(self.hash())
        print "Initialize ChordNode", self.id
        self.successor = int(self.id)
        self.successor_ip = self.ip
        self.predecessor = int(self.id)
        self.predecessor_ip = self.ip
        self.m = 8
        self.finger = {}
        self.init_finger_table()
        self.net = Net(self.ip, callbackMsgRcvd=self.message_received)
        self.next = -1
        self.start_mode = start_mode
        self.join_ip = join_ip
        print "start_mode:", self.start_mode, "join_ip", self.join_ip
        if self.start_mode == "join":
            self.join()
        else:
            self.successor = int(self.id)
        t = Thread(target=self.fix_fingers)
        t.start()

    def join(self):
        print "joining to", self.join_ip
        s, self.successor_ip = self.call_remote_proc(self.join_ip, "findSuccessor", str(self.id)).split()
        self.successor = int(s)

        p, self.predecessor_ip = self.call_remote_proc(self.successor_ip, "getPredecessor", "NONE").split()
        self.predecessor = int(p)

        self.call_remote_proc(self.predecessor_ip, "updateSuccessor", str(self.id) + "," + self.ip)
        self.call_remote_proc(self.successor_ip, "updatePredecessor", str(self.id) + "," + self.ip)

    def fix_fingers(self):
        time.sleep(3)
        while True:
            time.sleep(3)
            self.next += 1
            if self.next >= self.m:
                self.next = 0
            print self.id, ": fix_fingers", self.next, "s:",self.successor, "p:", self.predecessor
            s = self.find_successor((self.id + 2**(self.next))%256)
            print "fix_fingers", self.next, " rcvd s:", s
            self.finger[self.next].successor,self.finger[self.next].ip = s.split()
            #print "fix_fingers fixed to ", self.finger[self.next].ip
            for i in range(0, len(self.finger)):
                print i, " --- ", (self.id + 2**(i))%256, " --- ", self.finger[i].successor, " --- ", self.finger[i].ip
    def message_received(self, msg):
        #print "New msg arrived", "msg:", msg
        topic,data = msg.split()
        if topic == "findSuccessor":
            return self.find_successor(int(data))
        if topic == "getPredecessor":
            return str(self.predecessor) + " " + self.predecessor_ip
        if topic == "updateSuccessor":
            s, self.successor_ip = data.split(",")
            self.successor = int(s)
            return data
        if topic == "updatePredecessor":
            p, self.predecessor_ip = data.split(",")
            self.predecessor = int(p)
            return data
        return "0"

    def init_finger_table(self):
        for i in range(0, self.m):
            f = Finger()
            f.successor = int(self.id)
            f.ip = self.getIP()
            self.finger[i] = f

    def find_successor(self, id):
        #print "find_successor called for ", id, "self.successor", self.successor
        # if id < self.id:
        #     id = id + 256
        if self.id > int(self.successor) and self.id > id:
            if id + 256 in range(self.id, 256+int(self.successor)+1):
                #print "find_s 1"
                return str(self.successor) +" "+self.successor_ip
        elif self.id > int(self.successor):
            if id in range(self.id, 256+int(self.successor)+1):
                #print "find_s 2"
                return str(self.successor) +" "+self.successor_ip
        elif self.id > id:
            if id + 256 in range(self.id, int(self.successor)+1):
                #print "find_s 7"
                return str(self.successor) +" "+self.successor_ip
        elif id in range(self.id, int(self.successor)+1):
            #print "find_s 3"
            return str(self.successor) +" "+self.successor_ip
        n = self.closest_preceding_node(id)
        if n is None:
            #print "find_s 4"
            return str(self.id) + " " + self.ip
        if int(n.successor) == self.id:
            #print "find_s 5"
            n = Finger()
            n.successor = self.successor
            n.ip = self.successor_ip
            if int(n.successor) == self.id:
                return str(self.id) + " " + self.ip
        # if n.successor == self.successor:
        #     return str(self.id) + " " + self.ip
        #print "find_s 6"
        return self.call_remote_proc(n.ip, "findSuccessor", str(id))

    def closest_preceding_node(self, id):
        for i in range(self.m-1, -1, -1):
            #if self.id > id and self.id < self.finger[i].successor and self.finger[i].successor in range(self.id , 256+id):
            if self.id > id and self.id > int(self.finger[i].successor):
                s = int(self.finger[i].successor) + 256
                n_id = (256 + id)
                b = False
                if s > int(self.id) and s < n_id:
                    b = True
                #print "1 closest_preceding_node, i:", i, s, self.id, n_id, s in range(self.id, n_id), b
                # if s in range(self.id, n_id):
                if b:
                    return self.finger[i]
            elif self.id > id:
                s = int(self.finger[i].successor)
                n_id = (256 + id)
                b = False
                if s > int(self.id) and s < n_id:
                    b = True
                #print "2 closest_preceding_node, i:", i, s, self.id, n_id, s in range(self.id, n_id), b
                # if s in range(self.id, n_id):
                if b:
                    return self.finger[i]
            elif self.id > int(self.finger[i].successor):
                s = int(self.finger[i].successor) + 256
                n_id = id
                b = False
                if s > int(self.id) and s < n_id:
                    b = True
                #print "3 closest_preceding_node, i:", i, s, self.id, n_id, s in range(self.id, n_id), b
                # if s in range(self.id, n_id):
                if b:
                    return self.finger[i]
            elif int(self.finger[i].successor) in range(self.id, id):
                return self.finger[i]
        return None

    def call_remote_proc(self, ip, proc, data):
        #print "Calling remote proc of ", ip, " proc", proc, "with data:", data
        ret_msg = self.net.call_remote_procedure(ip, proc, data)
        #print "rcvd msg from RPC: ", ip, " proc", proc, "with data:", data, "rcvd:", ret_msg
        return ret_msg

    def hash(self):
        ring = HashRing(self.listID)
        id = ring.get_node(self.getIP())
        return id

    def getIP(self):
        return self.ip

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        ip = "127.0.0.1"
    if len(sys.argv) >= 3:
        start_mode = "join"
        join_ip = sys.argv[2]
        ChordNode(ip, start_mode, join_ip)
    else:
        ChordNode(ip)

#ChordNode()
