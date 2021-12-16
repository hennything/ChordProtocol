import socket, sys, hashlib, threading, pickle, time, random
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
        self.finger_table = []
        for i in range(MAX_BITS):
            x = pow(2, i)
            entry = (self.id + x) % pow(2, MAX_BITS)
            node = None
            self.finger_table.append([entry, node])

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
        threading.Thread(target = self.stabilize).start()
        threading.Thread(target=  self.fix_fingers).start()
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
        print("\nRequest listener data: ", data)
        connection.sendall(pickle.dumps(data))


    #####
    #
    #####
    def handle_request(self, msg):

        request = msg.split(":")[0]
        # print("request: ", request)
        print("handle request message: ", msg.split(":"))
        
        if request == "join_request":
            print("join_request called")
            # self.find_successor(id)
            data = msg.split(":")[1:]
            result = self.find_successor(data[2])

        if request == "get_successor":
            print("get_successor called")
            if self.succ == None:
                result = None
            else:
                result = self.succ

        if request == "get_predecessor":
            print("get_predecessor called")
            if self.pred == None:
                result = None
            else:
                result = self.pred
            # result = "NOT IMPLEMENTED JUST YET MATE"

        if request == "find_predecessor":
            print("find_predecessor called")
            data = msg.split(":")[1:]
            print(data)
            result = self.find_predecessor()

        if request == "notify":
            print("notify called")
            data = msg.split(":")[1:]
            print(data)
            self.notify(data[0], data[1], data[2])
            # filling result with arbritary thing so that this bad boy doesnt crash
            result = "notified"

        return result


    #####
    #
    #####
    def find_successor(self, id):
        
        if(id == self.id):
            return self.address

        predecessor = self.find_predecessor(id)
        if predecessor == None:
            return None

        print("MONITORING FOR ERROR: ", predecessor)
        data = self.request_handler.send_message(predecessor, "get_successor")
        return data


    #####
    #
    #####
    def find_predecessor(self, id):

        if(id == self.id):
            return self.address

        else:
            node_prime = self.closest_proceding_node(id)
            print("node_prime: ", node_prime)
            if node_prime == self.address:
                return self.address

            print("node_primt_2: ", node_prime)
            data = self.request_handler.send_message((node_prime), "find_predecessor:{}".format(self.id))
            return data
            # return node_prime

        # pass

    
    #####
    #
    #####
    def closest_proceding_node(self, id):
        node = None
        other = pow(2, MAX_BITS) + 1
        # print(other)

        for i in range(MAX_BITS-1, 0, -1):
            # print("PRINTING FINGER TABLE")
            # print(self.finger_table[i]) 
            # print(self.finger_table)
            if self.finger_table[i][1] is not None and self.id < self.finger_table[i][0] < int(id):
                print(i, self.finger_table[i])
                print("FINDING IT INSIDE THE LOOP FOR ONCE")
                return self.finger_table[i][1]
            # print(self.id, self.finger_table[i][1], id)
            # if self.finger_table[i][1] is not None and self.id < self.finger_table[i][1] < id:
                # print("FINGER LICKING GOOD TABLE: ", self.finger_table[i])
        
        return self.address


    #####   
    #
    #####
    def join(self, address):
        succ = self.request_handler.send_message(address, "join_request:{}:{}:{}".format(self.id, self.ip, self.port))
        print(succ)
        print("are we done finding out successor yet?")
        self.succ = succ
        self.finger_table[0][1] = self.succ
        self.pred = None

        self.succ_id = get_hash(succ[0] + ":" + str(succ[1]))
        # if self.succ != self.id:
            # data = self.request_handler.send_message(self.succ, "send_keys:{}".format(self.id))
        # self.successor = Node(ip,port)
        # address = self.get_address(succ)
        print("NODE: {} successfully joined the ring".format(self.id))



    def notify(self, id , ip , port):
        '''
        Recevies notification from stabilized function when there is change in successor
        '''
        print("I AM IN NOTIFY AND HERE IS WHERE I SHALL CRASH AND BURN")
        print(id, ip, port)
        if self.pred is not None:
            self.pred = (ip, int(port))
            return
        if self.pred is None or id > self.pred_id and id < self.id or self.id == self.pred and id != self.id:
            print("CONDITION TIME")
        # if self.pred is not None:
        #     if self.get_backward_distance(id) < self.get_backward_distance(self.pred):
        #         self.pred = (ip,int(port))
        #         return
        # if self.pred is None or self.pred == None or id > self.pred_id and id < self.id or self.id == self.pred and id != self.id:
        #     self.pred = (ip, int(port))
        #     if self.id == self.succ_id:
        #         self.succ = (ip, int(port))
        #         self.finger_table[0][1] = self.succ


    def fix_fingers(self):
        while True:
            random_index = random.randint(1,MAX_BITS-1)
            finger = self.finger_table[random_index][0]
            data = self.find_successor(finger)
            print("data: ", data)
            if data == None:
                time.sleep(10)
                continue
            # ip, port = data[0],  data[1]
            self.finger_table[random_index][1] = data 
            time.sleep(10)

            # while True:

            # random_index = random.randint(1,m-1)
            # finger = self.finger_table.table[random_index][0]
            # # print("in fix fingers , fixing index", random_index)
            # data = self.find_successor(finger)
            # if data == "None":
            #     time.sleep(10)
            #     continue
            # ip,port = self.get_ip_port(data)
            # self.finger_table.table[random_index][1] = Node(ip,port) 
            # time.sleep(10)


    def stabilize(self):
        # while True:
        #     time.sleep(5)
        #     ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     ping.connect(self.succ)
        #     ping.sendall(pickle.dumps([PREDECESSOR]))
        #     x_id, x_address = pickle.loads(ping.recv(4096))
        #     ping.close()
        #     if self.id < x_id < self.succ_id or self.succ_id == self.id:
        #         self.succ_id = x_id
        #         self.succ = x_address
        #     ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     ping.connect(self.succ)
        #     ping.sendall(pickle.dumps([self.id, self.address, NOTIFY]))
        #     ping.close()
        while True:
            if self.succ is None:
                time.sleep(10)
                continue

            if self.succ == self.address:
                time.sleep(10)

            # print("shouldnt even make it here")
            result = self.request_handler.send_message(self.succ, "get_predecessor")
            print("get predecessors: ", result)
            if result == None:
                self.request_handler.send_message(self.succ, "notify:{}:{}:{}".format(self.ip, self.port, self.id))
                continue

            # result = int(self.request_handler.send_message(ip,port,"get_id"))
            print(result)
            id = get_hash(result[0] + str(result[1]))
            print("ID BABY: ", id)

            print()
            print("===============================================")
            print("================= STABILIZING =================")
            print("===============================================")
            print()
            print("ID: ", self.id)
            if self.succ is not None:
                print("Successor ID: " , self.succ_id)
            if self.pred is not None:
                print("predecessor ID: " , self.pred_id)
            print()
            print("===============================================")
            print("=============== FINGER TABLE ==================")
            print(self.finger_table)
            print("===============================================")
            print()
            time.sleep(10)



if __name__ == '__main__':

    # create chord ring
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])

        node = Node(ip, port)
        # initial update of finger table when creating the chord ring
        node.pred = node.address
        node.succ = node.address
        node.finger_table[0][1] = (ip, port)
        node.start()

    # join chord ring
    if len(sys.argv) == 5:
        known_ip = sys.argv[1]
        known_port = int(sys.argv[2])

        ip = sys.argv[3]
        port = int(sys.argv[4])
        
        node = Node(ip, port)
        node.join((known_ip, known_port))
        node.start()
        # print(node.succ)
        # print(node.pred)

     



    
