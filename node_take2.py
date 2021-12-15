import socket, sys, hashlib, threading, pickle
from collections import OrderedDict
from request_handler import RequestHandler

LOOKUP = 'lookup'
JOIN = 'join'


MAX_BITS = 10
def get_hash(key):
    result = hashlib.sha256(key.encode())
    return int(result.hexdigest(), 16) % pow(2, MAX_BITS)


class Node:

    def __init__(self, ip, port):
        '''
        storage:
        '''
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        
        # a nodes identifier is chosen by hashing the nodes IP address - paper
        # we use a combination of ip and port during testing otherwise all nodes will have the same ip
        self.id = get_hash(self.ip + ":" + str(self.port))
        self.finger_table = OrderedDict()
        for i in range(MAX_BITS):
            x = pow(2, i)
            entry = (self.id + x) % pow(2, MAX_BITS)
            node = None
            self.finger_table[entry] = node

        # print(self.finger_table)
        self.pred = None
        self.pred_id = None
        self.succ = None
        self.succ_id = None

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

        self.request_handler = RequestHandler()
    
    #####
    #
    #####
    def start(self):
        print("Starting node: ", self.id)
        while True:
            connection, address = self.socket.accept()
            connection.settimeout(60)
            threading.Thread(target=self.request_listener, args=(connection, address)).start()


    #####
    #
    #####
    def request_listener(self, connection, address):
        
        data = pickle.loads(connection.recv(1024))

        data = self.handle_request(data)
        print("Request listener data: ", data)
        connection.sendall(pickle.dumps(data))


    #####
    #
    #####
    def handle_request(self, data):

        request = data.split()[0]
        print("request: ", request)
        
        if request == "join_request":
            result = self.join_request(data[0], data[1])
            print(result)

        if request == "get_successor":
            if self.succ == None:
                result = self.id
            else:
                result = self.succ
            # result = self.succ
            # return self.succ 

        if request == "find_predecessor":
            print("GOTTA GO FIND THAT MOTHERFUCKER")

        return result
        # return result
        # if operation == "join_request":
        #     # print("join request recv")
        #     result  = self.join_request_from_other_node(int(args[0]))
        # if data == JOIN:
        #     pass

        # pass

    #####
    #
    #####
    def find_successor(self, id):
        
        if(id == self.id):
            print("found first")
            return self.address

        # print("looking for predecessor")
        predecessor = self.find_predecessor(id)
        data = self.request_handler.send_message(predecessor, "get_successor")
        return data
        # print(data)
        # print(predecessor)
        # pass


    #####
    #
    #####
    def find_predecessor(self, id):

        if(id == self.id):
            # print("found first")
            return self.address

        else:
            # print("looking for closest proceding node")
            node_prime = self.find_closest_proceding_node(id)
            # print(node_prime)
            if node_prime == self.address:
                return self.address

            data = self.request_handler.send_message((node_prime), "find_predecessor {}".format(self.id))
            return data
            # return node_prime

        # pass

    
    #####
    #
    #####
    def find_closest_proceding_node(self, id):
        node = None
        m = pow(2, MAX_BITS) + 1

        for i in range(m, 1, -1):
            if i in self.finger_table and self.finger_table[i] is not None and self.id < get_hash(self.finger_table[i][0] + ":" + str(self.finger_table[i][1])) < id:
                # print(get_hash(self.finger_table[i][0] + ":" + str(self.finger_table[i][1])))
                # print(self.finger_table[i])
                return self.finger_table[i]

        return self.address



    #####   
    #
    #####
    def join(self, address):
        succ = self.request_handler.send_message(address, "join_request {}".format(self.address))
        print(succ)
        print("are we done finding out successor yet?")
        # address = self.get_address(succ)


    #####
    #
    #####
    def join_request(self, address, id):
        return self.find_successor(id)

if __name__ == '__main__':

    # create chord ring
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])

        node = Node(ip, port)
        # initial update of finger table when creating the chord ring
        node.finger_table[0] = (ip, port)
        node.start()

    # join chord ring
    if len(sys.argv) == 5:
        known_ip = sys.argv[1]
        known_port = int(sys.argv[2])

        ip = sys.argv[3]
        port = int(sys.argv[4])
        
        node = Node(ip, port)
        node.join((known_ip, known_port))
        print("will we ever start this fucking node")
        node.start()
        # print(node.succ)
        # print(node.pred)

     



    
