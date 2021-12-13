import socket, sys, hashlib, threading, pickle
from collections import OrderedDict

LOOKUP = 'lookup'


# https://en.wikipedia.org/wiki/Chord_(peer-to-peer)
# Takes key string, uses SHA-1 hashing and returns a 10-bit (1024) compressed integer
# TODO: rewrite
MAX_BITS = 10 # TODO: change this
MAX_NODES = 2 ** MAX_BITS # TODO: change this

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

        # straight from Geeks4Geeks
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
        threading.Thread(target=self.request_listener, args=()).start()
        # while true here or we can get a result from menu to stop the loop based on the user's choice
        # everytime a user makes a choice at the menu function, the necessary functions are called
        # then we return here and call again and wait for more input
        while True:
            self.menu()
        # need more code here
        # pass

    def request_listener(self):
        '''
        Listen to incoming requests, we need to call another thread here, because if we
        call the necessary functions, it means we block other incoming requests
        :return:
        '''
        while True:
            try:
                connection, address = self.socket.accept()
                # do we need the timeout?
                connection.settimeout(120)
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
        print(data)
        try:
            # if data[-1] == LOOKUP:
            succ, succ_id = self.find_successor(data[0], data[1])
            print(succ, succ_id)
        except:
            # print("no load")
            pass
        # print(connection)
        # print(address)
        # pass

    def menu(self):
        '''
        responsible for the menu of the client
        called by the start_node function
        :return:
        '''
        self.print_menu()
        mode = input()
        if mode == '1':
            # call join network
            # first get the IP and Port of the known node in the network
            print("Give the IP of the known node:")
            known_ip = input()
            print("Give the port of the known node you want to connect:")
            known_port = input()
            self.join(known_ip, known_port)
            # pass
        elif mode == '2':
            # leave the network
            pass
        elif mode == '3':
            #     quit completely?
            pass
        elif mode == '4':
            print(self.pred)
        elif mode == '5':
            print(self.succ)
        pass

    # NOTE: function to join node to network
    def join(self, ip, port):
        '''
        '''
        ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ping.connect((ip, int(port)))
        ping.sendall(pickle.dumps([self.address, self.id, LOOKUP]))
        # pred = pickle.loads(ping.recv(4096))
        # print(pred)

        # self.update_finger_table()

    def leave(self):
        '''
        Leave the network
        :returns nothing
        '''
        self.pred = (self.ip, self.port)
        self.pred_id = self.id
        self.succ = (self.ip, self.port)
        self.succ_id = self.id
        self.finger_table.clear()

    def stabilize(self):
        '''
        verifys successor and notifys successor
        '''
        pass

    # NOTE: just pings successor i think, confirm this
    def notify(self, node):
        '''
        updates predecessor node
        '''
        pass

    def update_finger_table(self):
        '''
        does what the name says
        should call find successor (maybe)
        '''
        for i in range(MAX_BITS):
            entry_id = (self.id + (2 ** i)) % MAX_NODES
            # If only one node in network
            if self.succ == self.address:
                self.finger_table[entry_id] = (self.id, self.address)
                continue
            # If multiple nodes in network, we find succ for each entryID
            # recvIPPort = self.getSuccessor(self.succ, entryId)
            # recvId = getHash(recvIPPort[0] + ":" + str(recvIPPort[1]))
            # self.finger_table[entryId] = (recvId, recvIPPort)
        # pass

    # TODO: review
    def update_all_finger_tables(self):
        succ = self.succ
        while True:
            if succ == self.address:
                break
            try:
                peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer.connect(succ)  # Connecting to server
                peer.sendall(pickle.dumps())
                succ = pickle.loads(peer.recv(4096))
                peer.close()
                if succ == self.succ:
                    break
            except socket.error:
                print("Connection denied")

    def check_predecessor(self):
        '''
        check if predecessor node still exists
        '''
        pass


    # the first node on the ring with id greater than or equal id
    def find_successor(self, address, id): 
        '''
        returns -> [ip, port]
        '''
        # print(address)
        # print(id)
        if id > self.id and id <= self.succ_id:
            return self.succ, self.succ_id
        else:
            print("running closest_proceding_node")
            succ = self.closest_preceding_node(id)
            # succ = ('127.0.0.1', 8000)
            ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ping.connect(succ)
            # ping.sendall(pickle.dumps([self.address, self.id, LOOKUP]))

            # return n0.find_successor(id)
            # return "ASK SOMEONE ELSE"

           

    def closest_preceding_node(self, id):
        '''
        search the local table for the highest predecessor of id
        :return:
        '''
        # TODO: add the finger tabless
        for i in range(5, 1, -1):
            if self.finger_table[i] > self.id and self.finger_table[i] < id:
                print(self.finger_table[i])
                return self.finger_table[i]
        return self.address

        

    def update_successor(self):
        '''
        Responsible for updating the successor in case it wasn't found
        :return:
        '''
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


if __name__ == '__main__':

    # get IP and PORT
    if len(sys.argv) < 3:
        print("Not enough arguments given")
        print("Initializing to IP: 127.0.0.1 and port 8080")
        IP = "127.0.0.1"
        PORT = 8080
    else:
        IP = sys.argv[1]
        PORT = int(sys.argv[2])

    node_1 = Node(IP, PORT)
    print("Node ID: ", node_1.id)
    node_1.start_node()
    node_1.print_finger_table()
