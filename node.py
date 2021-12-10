import socket, random, sys, hashlib, os, threading
from collections import OrderedDict


MAX_BITS = 10 # TODO: change this
MAX_NODES = 2 ** MAX_BITS # TODO: change this

# Takes key string, uses SHA-512 hashing and returns a 10-bit (1024) compressed integer
# TODO: rewrite
def get_hash(key):
    result = hashlib.sha512(key.encode())
    return int(result.hexdigest(), 16) % MAX_NODES

class Node:

    def __init__(self, ip, port):
        '''
        storage:
        '''
        self.ip = ip
        self.port = port
        self.id = get_hash(self.ip + ":" + str(self.port))
        self.finger_table = OrderedDict()
        
        self.pred = (ip, port)
        self.pred_id = self.id
        self.succ = (ip, port)
        self.succ_id = self.id

        # straight from Geeks4Geeks
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((self.ip, self.port))
            self.socket.listen()
        except socket.error as massage:
            # if any error occurs then with the 
            # help of sys.exit() exit from the program
            print('Bind failed. Error Code : ' + str(massage[0]) + ' Message ' + massage[1])
            sys.exit()

    # NOTE: function to join node to network
    def join(self):
        '''
        '''
        pass

    def create(self):
        '''
        '''
        pass
        # self.pred_id = None
        # self.succ_id = n

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


    def print_finger_table(self):
        for key, value in self.finger_table.items(): 
            print("KeyID:", key, "Value", value)


if __name__ == '__main__':

    IP = "127.0.0.1"
    PORT = 8080

    node_1 = Node(IP, PORT)
    print("Node ID: ", node_1.id)
    node_1.print_finger_table()