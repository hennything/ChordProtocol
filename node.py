import socket, random, sys, hashlib, os
from collections import OrderedDict

MAX_BITS = 10        # 10-bit
MAX_NODES = 2 ** MAX_BITS
# Takes key string, uses SHA-1 hashing and returns a 10-bit (1024) compressed integer
def getHash(key):
    result = hashlib.sha1(key.encode())
    return int(result.hexdigest(), 16) % MAX_NODES

class Node:

    def __init__(self, ip, port):
        '''
        storage:
        '''
        self.ip = ip
        self.port = port
        self.id = getHash(self.ip + ":" + str(self.port))
        self.finger_table = OrderedDict()
        
        self.pred = (ip, port)
        self.pred_id = self.id
        self.succ = (ip, port)
        self.succ_id = self.id

    def create(self):
        '''
        
        '''
        pass
        # self.pred_id = None
        # self.succ_id = n
    
    def join(self, node):
        '''

        '''
        pass

    def stabilize(self):
        '''
        verifys successor and notifys successor
        '''
        pass

    def notify(self, node):
        '''
        updates predecessor node
        '''
        pass

    def fix_fingers(self):
        '''

        '''
        pass

    def check_predecessor(self):
        '''
        check if predecessor node still exists
        '''
        pass

    def find_successor(self, id):
        '''

        '''
        pass


if __name__ == '__main__':

    IP = "127.0.0.1"
    PORT = 8080

    node_1 = Node(IP, PORT)
    print("Node ID: ", node_1.id)