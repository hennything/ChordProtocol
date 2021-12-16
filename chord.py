import hashlib
import pickle
import socket
import sys
import threading
import time
from collections import OrderedDict

LOOKUP = 'lookup'
PREDECESSOR = 'predecessor'
NOTIFY = 'notify'

MAX_BITS = 10
MAX_NODES = 2 ** MAX_BITS


def get_hash(key):
    result = hashlib.sha1(key.encode())
    return int(result.hexdigest(), 16) % MAX_NODES


class Node:

    def __init__(self, ip, port):
        '''
        storage:
        '''
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        self.id = get_hash(self.ip + ":" + str(self.port))
        self.finger_table = OrderedDict()
        self.pred = (ip, port)
        self.pred_id = self.id
        self.succ = (ip, port)
        self.succ_id = self.id
        self.succ_list = []

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((self.ip, self.port))
            self.socket.listen()
        except socket.error as msg:
            # if any error occurs then with the
            # help of sys.exit() exit from the program
            print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

    def start_node(self):
        '''
        function to initially start the client/node -> should give the menu for connecting
        to the network or leaving the network
        we should call the listener for any requests
        '''
        threading.Thread(target=self.fix_fingers, args=()).start()
        threading.Thread(target=self.stabilize, args=()).start()
        threading.Thread(target=self.request_listener, args=()).start()
        while True:
            self.menu()

    def request_listener(self):
        '''
        Listen to incoming requests, we need to call another thread here, because if we
        call the necessary functions, it means we block other incoming requests
        :return:
        '''
        while True:
            try:
                connection, address = self.socket.accept()
                connection.settimeout(60)
                threading.Thread(target=self.handle_request, args=(connection, address)).start()
            except socket.error:
                print("An error occured")

    def handle_request(self, connection, address):
        '''
        here we handle the actual requests. Depending on the payload, we do different things
        First of all, finding successor, the paper defines the function find_successor
        :param connection: connection object
        :param address: ip address of incoming request
        :return:
        '''
        data = pickle.loads(connection.recv(4096))
        try:
            if data[-1] == LOOKUP:
                succ_id, succ_node = self.find_successor(data[0], data[1])
                connection.sendall(pickle.dumps([succ_node, succ_id]))
            elif data[-1] == PREDECESSOR:
                connection.sendall(pickle.dumps([self.pred_id, self.pred]))
            elif data[-1] == NOTIFY:
                self.notify(data[0], data[1])
        except socket.error:
            print("An error occurred in handle request")

    def join(self, ip, port):
        ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ping.connect((ip, int(port)))
        ping.sendall(pickle.dumps([self.address, self.id, LOOKUP]))
        node_address, node_id = pickle.loads(ping.recv(4096))
        self.succ = node_address
        self.succ_id = node_id

    def find_successor(self, address, node_id):
        if self.id < node_id < self.succ_id or self.succ_id == self.id:
            return self.succ_id, self.succ
        else:
            finger = self.closest_preceding_node(node_id)
            try:
                ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ping.connect(finger[1])
                ping.sendall(pickle.dumps([finger[1], finger[0]]))
                node_id, node = pickle.loads(ping.recv(4096))
                return node_id, node
            except socket.error:
                print("Socket error at find successor")

    def closest_preceding_node(self, node_id):
        for i in range(MAX_BITS, 1, -1):
            if len(self.finger_table) >= i and self.id < self.finger_table[i][0] < node_id:
                return self.finger_table[i]
        return self.id, self.address

    def fix_fingers(self):
        while True:
            time.sleep(5)
            for i in range(1, MAX_BITS):
                finger_id = self.id + 2**(i - 1)
                node_id, node = self.find_successor(self.succ, finger_id)
                self.finger_table[i] = [node_id, node]

    def notify(self, node_id, node_address):
        if self.pred_id == self.id or self.pred_id < node_id < self.id:
            self.pred_id = node_id
            self.pred = node_address

    def stabilize(self):
        while True:
            time.sleep(5)
            ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ping.connect(self.succ)
            ping.sendall(pickle.dumps([PREDECESSOR]))
            x_id, x_address = pickle.loads(ping.recv(4096))
            ping.close()
            if self.id < x_id < self.succ_id or self.succ_id == self.id:
                self.succ_id = x_id
                self.succ = x_address
            ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ping.connect(self.succ)
            ping.sendall(pickle.dumps([self.id, self.address, NOTIFY]))

    def menu(self):
        '''
        responsible for the menu of the client
        called by the start_node function
        :return:
        '''
        self.print_menu()
        mode = input()
        if mode == '1':
            print("Give the IP of the known node:")
            known_ip = input()
            print("Give the port of the known node you want to connect:")
            known_port = input()
            self.join(known_ip, known_port)
        elif mode == '2':
            pass
        elif mode == '3':
            self.print_finger_table()
        elif mode == '4':
            print(self.pred)
        elif mode == '5':
            print(self.succ)
        pass

    def print_menu(self):
        print("""\n1. Join Network
                 \n2. Leave Network
                 \n3. Print Finger Table
                 \n4. Print Predecessor
                 \n5. Print Successor""")

    def print_predecessor(self):
        print("Predecessor:", self.pred_id)

    def print_successor(self):
        print("Successor:", self.succ_id)

    def print_finger_table(self):
        for key, value in self.finger_table.items():
            print("KeyID:", key, "Value", value)


# get IP and PORT
if len(sys.argv) < 3:
    print("Not enough arguments given")
    print("Initializing to IP: 127.0.0.1 and port 8080")
    IP = "127.0.0.1"
    PORT = 8080
else:
    IP = sys.argv[1]
    PORT = int(sys.argv[2])

node = Node(IP, PORT)
print("I am node ", str(node.id), " and my address is ", str(node.address))
node.start_node()
