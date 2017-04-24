from kazoo.client import KazooClient
from kazoo.client import KazooState
from time import sleep

zk = KazooClient(hosts='10.0.0.1:2181')

class ZooState:
    status = ""
def my_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        ZooState.status = "Connection to zookeeper server lost"
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        ZooState.status = "Connection to zookeeper server suspended"
    else:
        # Handle being connected/reconnected to Zookeeper
        ZooState.status = "Connection to zookeeper server established"
    print ZooState.status
zk.add_listener(my_listener)
zk.start()


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

get(zk)
create_pub(zk)
get(zk)

#zk.stop()

while raw_input() != "q":
    sleep(0.1)
    # if ZooState.status != "":
    #     print ZooState.status
    #     ZooState.status = ""
    continue

zk.stop()