import hashlib
import pickle
import socket
import sys
import threading
import time

from request_handler import RequestHandler

MAX_BITS = 4

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
            self.finger_table.append([self.id, self.address])

        self.pred = None
        self.succ = self.address
        self.pred_id = None
        self.succ_id = self.id

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((self.ip, self.port))
            self.socket.listen()
        except socket.error as msg:
            print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        self.request_handler = RequestHandler()

    #####
    #
    #####
    def start(self):
        threading.Thread(target=self.stabilize).start()
        threading.Thread(target=self.fix_fingers).start()
        threading.Thread(target=self.check_predecessor).start()
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
        if data is None:
            print(connection)
        print("\nRequest listener data: ", data)
        connection.sendall(pickle.dumps(data))

    def handle_request(self, msg):

        request = msg.split(":")[0]
        print("handle request message: ", msg.split(":"))

        if request == "join_request":
            print("join_request called")
            # self.find_successor(id)
            data = msg.split(":")[1:]
            result = self.find_successor(data[2])

        if request == "find_successor":
            print("find_successor called")
            data = msg.split(":")[1:]
            print("This is the id we are looking", data[0])
            result = self.find_successor(data[0])


        if request == "get_successor":
            print("get_successor called")
            # if self.succ == None:
            #     result = None
            # else:
            result = self.succ

        if request == "get_predecessor":
            print("get_predecessor called")
            # if self.pred == None:
            #     print("but your getting none in return hoe")
            #     result = None
            # else:
            print(self.pred_id, self.pred)
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

        if request == "ping":
            # print("I was pinged")
            result = "pinged"

        return result

    def find_successor(self, id):
        if self.id < int(id) < self.succ_id or self.succ_id == self.id:
            return self.succ
        else:
            print("Find successor second part")
            print(id)
            finger = self.closest_preceding_node(id)
            if finger == self.address:
                return self.address
            if id is None:
                print("ID WAS NONE")
            address = self.request_handler.send_message(finger, "find_successor:{}".format(id))
            if address == "error":
                return self.address
            return address

    def closest_preceding_node(self, id):
        for i in range(MAX_BITS-1, 0, -1):
            if self.finger_table[i][1] is not None and self.id < self.finger_table[i][0] < int(id):
                return self.finger_table[i][1]
        return self.address

    def find_predecessor(self, id):
        if id == self.id:
            return self.address
        else:
            node_prime = self.closest_preceding_node(id)
            if node_prime == self.address:
                return self.address
            data = self.request_handler.send_message((node_prime), "find_predecessor:{}".format(self.id))
            if data == "error":
                return self.address
            return data

    def join(self, address):
        succ = self.request_handler.send_message(address,
                                                 "join_request:{}:{}:{}".format(self.id, self.ip, self.port))
        # this call shouldnt even get the error
        if succ == "error":
            # execute leave
            pass
        self.succ = succ
        self.succ_id = get_hash(succ[0] + ":" + str(succ[1]))
        self.finger_table[0][0] = self.succ_id
        self.finger_table[0][1] = self.succ
        # self.pred = None
        print("Node {} successfully joined the Chord ring".format(self.succ_id))

    def notify(self, id , ip , port):
        '''
        Recevies notification from stabilized function when there is change in successor
        '''
        if self.pred is None or self.pred == self.address or int(self.pred_id) < int(id) < int(self.id) or \
                (int(self.pred_id) > int(self.id) > int(id)):
            self.pred = (ip, int(port))
            self.pred_id = get_hash(ip + ":" + port)
            print("CONDITION TIME")

    def fix_fingers(self):
        while True:
            for i in range (1, MAX_BITS):
                finger = self.finger_table[i][0]
                self.finger_table[i][1] = self.find_successor(finger)
                id = get_hash(ip + ":" + str(port))
                self.finger_table[i][0] = id
            # random_index = random.randint(1,MAX_BITS-1)
            # finger = self.finger_table[random_index][0]
            # data = self.find_successor(finger)
            # if data == None:
            #     time.sleep(10)
            #     continue
            # self.finger_table[random_index][1] = data
            time.sleep(10)

    def stabilize(self):
        while True:
            if self.succ is None:
                time.sleep(10)
                continue
            if self.succ == self.address:
                time.sleep(10)
            result = self.request_handler.send_message(self.succ, "get_predecessor")
            
            if result == "error":
                self.succ_id = self.id
                self.succ = self.address
                # result = [self.id, self.address]
            elif result[0] is not None:
                id = get_hash(result[1][0] + ":" + str(result[1][1]))
                if int(self.id) < int(id) < int(self.succ_id) or int(self.succ_id) == int(self.id) or \
                            (int(self.succ_id) < int(self.id) and int(self.succ_id > int(id))):
                    self.succ_id = id
                    self.succ = (result[1][0], result[1][1])
            print("CALLING NOTIFY ON ", self.succ)
            self.request_handler.send_message(self.succ, "notify:{}:{}:{}".format(self.id, self.ip, self.port))
            print()
            print("===============================================")
            print("================= STABILIZING =================")
            print("===============================================")
            print()
            print("ID/Address: ", self.id, self.address)
            if self.succ is not None:
                print("Successor ID/Address: " , self.succ_id, self.succ) 
            if self.pred is not None:
                print("predecessor ID/Address: " , self.pred_id,  self.pred)
            print()
            print("===============================================")
            print("=============== FINGER TABLE ==================")
            print(self.finger_table)
            print("===============================================")
            print()
            time.sleep(10)

    def check_predecessor(self):
        while True:
            time.sleep(10)
            if self.pred is None or self.pred == self.address:
                continue
            ping_result = self.request_handler.send_message(self.pred, "ping")
            print("pinged results: ", ping_result)
            if ping_result == "pinged":
                continue
            self.pred = None
            self.pred_id = None
            print("pinged: ", self.pred, self.pred_id)


if __name__ == '__main__':

    # create chord ring
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])

        node = Node(ip, port)
        # node.finger_table[0][1] = (ip, port)
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