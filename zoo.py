from kazoo.client import KazooClient
from kazoo.client import KazooState
from time import sleep
import pprint
import logging
logging.basicConfig()
from threading import Thread

class Kazoo:
    def __init__(self, zoo_ip, self_ip, listener = None):
        self.status = ""
        self.self_ip = self_ip
        pprint.pprint("starting Kazoo client")
        if zoo_ip == None:
            zoo_ip = '10.0.0.1:2181'
        self.zoo_ip = zoo_ip
        pprint.pprint("zoo_ip: " + zoo_ip)
        if listener == None:
            listener = self.my_listener
        self.zoo = KazooClient('10.0.0.1:2181')
        self.zoo.add_listener(self.my_listener)
        self.zoo.start()
        pprint.pprint("kazoo started")

        self.zoo.ensure_path('electionpath')
        self.zoo.create('electionpath/' + self_ip, ephemeral=True)
        self.election = self.zoo.Election('electionpath', self_ip)

        contenders = self.election.contenders()
        if len(contenders) == 1:
            if self.zoo.exists("leader_info") != None:
                self.zoo.delete("leader_info", recursive=True)

        self.im_the_leader = False
        if self.zoo.exists("leader_info") == None:
            self.election.run(self.my_leader_function)
        else:
            self.watch_for_leader()

        # @self.zoo.ChildrenWatch("electionpath/")
        # def watch_children(children):
        #     if self.im_the_leader:
        #         return
        #     print "election path changed, new election is on..!"
        #     election.run(self.my_leader_function)

    def watch_for_leader(self):
        leader = self.zoo.get("leader_info")
        self.leader_ip, stat = leader
        print "Leader IP: ", self.leader_ip
        self.zoo.get("electionpath/" + self.leader_ip, watch=self.run_election)

    def run_election(self, event):
        contenders = self.zoo.get_children('electionpath')
        if len(contenders) > 1:
            print "contenders:", contenders
            if contenders[0] == self.self_ip:
                print "calling to run election again"
                self.election.run(self.my_leader_function)
        else:
            print "calling to run election again"
            self.election.run(self.my_leader_function)
        self.zoo.get("leader_info", watch=self.election_finished)


    def election_finished(self, event):
        if self.im_the_leader:
            return
        self.watch_for_leader()

    def my_leader_function(self):
        print "my_leader_function started, ", "im the leader"
        self.im_the_leader = True
        self.leader_ip = self.self_ip
        leader = self.zoo.exists("leader_info")
        if leader == None:
            self.zoo.create('leader_info', self.self_ip)
        else:
            self.zoo.set('leader_info', self.self_ip)

    def stop(self):
        self.zoo.stop()
        pprint.pprint("kazoo stopped")


    def my_listener(self, state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            self.status = "Connection to zookeeper server lost"
        elif state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            self.status = "Connection to zookeeper server suspended"
        else:
            # Handle being connected/reconnected to Zookeeper
            self.status = "Connection to zookeeper server established"
        pprint.pprint(self.status)

def create_pub(zk):
    # Ensure a path, create if necessary
    topic = "topic_default"
    path = "/pubs/"+topic
    zk.ensure_path(path)

    # Create a node with data
    node_id = 2
    zk.create(path+"/"+str(node_id), b"10.0.0.2")

def get(zk):
    topic = "topic_default"
    path = "/pubs/" + topic
    # Determine if a node exists
    if zk.exists(path):
    # Do something
        print "exists:", path

        # Print the version of a node and its data
        data, stat = zk.get(path)
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

        # List the children
        children = zk.get_children(path)
        print("There are %s children with names %s" % (len(children), children))
        for c in children:
            data, stat = zk.get(path + "/" + c)
            print c, " -> ", data.decode("utf-8")
    else:
        print "does not exist:", path

# get(zk)
# create_pub(zk)
# get(zk)

#zk.stop()

# while raw_input() != "q":
#     sleep(0.1)
#     # if ZooState.status != "":
#     #     print ZooState.status
#     #     ZooState.status = ""
#     continue
#
# zk.stop()