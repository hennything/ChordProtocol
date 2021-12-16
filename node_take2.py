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
            # connection.settimeout(60)
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
        print("handle request message: ", msg.split(":"))

        if request == "send_keys":
            data = msg.split(":")[1:]
            print("send_keys called")
            print(data)
            return "AAAAHAHAHAHH"
        
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
                print("but your getting none in return hoe")
                result = None
            else:
                result = [self.pred_id, self.pred]

        if request == "find_predecessor":
            print("find_predecessor called")
            data = msg.split(":")[1:]
            print(data)
            result = self.find_predecessor(data[0])

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
        # print("calling get_successor with predecessor: ", predecessor)
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
            if node_prime == self.address:
                return self.address
            data = self.request_handler.send_message((node_prime), "find_predecessor:{}".format(self.id))
            return data
            # return node_prime

        #     if search_id == self.id:
        #     return str(self.nodeinfo)
        # # print("finding pred for id ", search_id)
        # if self.predecessor is not None and  self.successor.id == self.id:
        #     return self.nodeinfo.__str__()
        # if self.get_forward_distance(self.successor.id) > self.get_forward_distance(search_id):
        #     return self.nodeinfo.__str__()
        # else:
        #     new_node_hop = self.closest_preceding_node(search_id)
        #     # print("new node hop finding hops in find predecessor" , new_node_hop.nodeinfo.__str__() )
        #     if new_node_hop is None:
        #         return "None"
        #     ip, port = self.get_ip_port(new_node_hop.nodeinfo.__str__())
        #     if ip == self.ip and port == self.port:
        #         return self.nodeinfo.__str__()
        #     data = self.request_handler.send_message(ip , port, "find_predecessor|"+str(search_id))
        #     return data

        # pass

    
    #####
    #
    #####
    def closest_proceding_node(self, id):
        node = None
        other = pow(2, MAX_BITS) + 1
        for i in range(MAX_BITS-1, 0, -1):
            if self.finger_table[i][1] is not None and self.id < self.finger_table[i][0] < int(id):
                return self.finger_table[i][1]
        return self.address


    #####   
    #
    #####
    def join(self, address):
        print("JOIN")
        succ = self.request_handler.send_message(address, "join_request:{}:{}:{}".format(self.id, self.ip, self.port))
        # print(get_hash(succ[0] + ":" + str(succ[1])))
        self.succ = succ
        self.succ_id = get_hash(succ[0] + ":" + str(succ[1]))
        self.finger_table[0][1] = self.succ
        self.pred = None


    def notify(self, id , ip , port):
        '''
        Recevies notification from stabilized function when there is change in successor
        '''
        print("NOTIFY NONE OF THE CONDITIONS HAVE BEEN MET YET: ", id, ip, port)
        if self.pred is not None:
            self.pred = (ip, int(port))
            print("notify: ", self.pred)
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
            if data == None:
                time.sleep(10)
                continue
            self.finger_table[random_index][1] = data 
            time.sleep(10)


    def stabilize(self):
        while True:
            if self.succ is None:
                time.sleep(10)
                continue
            if self.succ == self.address:
                time.sleep(10)
            print("self successor: ", self.succ)
            result = self.request_handler.send_message(self.succ, "get_predecessor")
            print("CALELD GET_PREDECESSOR: ", result)
            if result[0] == None:
                print("DO WE MAKE IT IN HERE?")
                self.request_handler.send_message(self.succ, "notify:{}:{}:{}".format(self.id, self.ip, self.port))
                # continue

            id = get_hash(result[1][0] + ":" + str(result[1][1]))
            print("ID BABY: ", id)

            print()
            print("===============================================")
            print("================= STABILIZING =================")
            print("===============================================")
            print()
            print("ID/Address: ", self.id, self.address)
            if self.succ is not None:
                print("Successor ID/Address: " , self.succ_id, self.succ) # TODO: remove self.succ
            if self.pred is not None:
                print("predecessor ID/Address: " , self.pred_id,  self.pred) # TODO: remove self.pred
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

     



    
