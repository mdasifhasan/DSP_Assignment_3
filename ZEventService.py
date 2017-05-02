from zoo import *
import sys
import pprint

class ZKeventService:
    def __init__(self, self_ip = None, zoo_ip = None, listener = None):
    	pprint.pprint("starting event service")
    	self.kazoo = Kazoo(zoo_ip, self_ip)
        pprint.pprint("event service started")

    def stop(self):
     	self.kazoo.stop()
        pprint.pprint("event service stopped")

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