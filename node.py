import hashlib
import pickle
import socket
import sys
import threading
import time
import random

from request_handler import RequestHandler

MAX_BITS = 4
SLEEP_TIME = 5

def get_hash(key):
    result = hashlib.sha256(key.encode())
    return int(result.hexdigest(), 16) % pow(2, MAX_BITS)


class Node:

    def __init__(self, ip, port, listen_param=None):
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
        self.init_finger_table()

        self.pred = None
        self.succ = self.address
        self.pred_id = None
        self.succ_id = self.id
        self.threads = []
        self.run_threads = True

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((self.ip, self.port))
            if listen_param != None:
                self.socket.listen(listen_param)
            else:
                self.socket.listen()
        except socket.error as msg:
            print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        self.request_handler = RequestHandler()

    def init_finger_table(self):
        """
        just initializes the finger table to entries pointing to self
        """
        for i in range(MAX_BITS):
            x = pow(2, i)
            entry = (self.id + x) % pow(2, MAX_BITS)
            self.finger_table.append([entry, None])
        # for i in range(MAX_BITS):
        #     self.finger_table.append([self.id, self.address])

    def start(self):
        """
        Launches all needed threads. It launches the threads and keeps track of them in the threads
        list of the node. Launches a thread for: stabilize, fix fingers, check predecessor, menu and request listener.
        :return:
        """
        self.finger_table[0][1] = self.succ

        t_stab = threading.Thread(target=self.stabilize)
        t_stab.start()
        self.threads.append(t_stab)
        t_fix = threading.Thread(target=self.fix_fingers)
        t_fix.start()
        self.threads.append(t_fix)
        t_check = threading.Thread(target=self.check_predecessor)
        t_check.start()
        self.threads.append(t_check)

        t_menu = threading.Thread(target=self.menu)
        t_menu.start()
        self.threads.append(t_menu)

        while self.run_threads:
            connection, address = self.socket.accept()
            th = threading.Thread(target=self.request_listener, args=(connection, address))
            th.start()
            self.threads.append(th)

    def request_listener(self, connection, address):
        """
        when another node makes a connection with us, a thread is launched that runs this function. It is responsible
        of running the handle request method and sending back the results
        :param connection: The connection object created when we received a request
        :param address: The address of the node that we handle the request for
        :return: The method does not return something back. It just sends back the result with the socket connection
        """
        data = pickle.loads(connection.recv(1024))
        data = self.handle_request(data)
        connection.sendall(pickle.dumps(data))

    def handle_request(self, msg):
        """
        Responsible for calling the right method depending on the data of the message that was sent as a request
        :param msg: The Message that was sent as a request
        :return: returns the result created when handling the request
        """
        request = msg.split(":")[0]
        result = ''
        if request == "join":
            data = msg.split(":")[1:]
            result = self.find_successor(data[2])

        if request == "find_successor":
            data = msg.split(":")[1:]
            result = self.find_successor(data[0])

        if request == "get_successor":
            result = self.succ

        if request == "find_predecessor":
            data = msg.split(":")[1:]
            result = self.find_predecessor(data[0])

        if request == "get_predecessor":
            result = [self.pred_id, self.pred]

        if request == "notify":
            data = msg.split(":")[1:]
            self.notify(data[0], data[1], data[2])
            result = "notified"

        if request == "ping":
            result = "pinged"

        return result

    def find_successor(self, id):
        """
        Find successor method, follows what was the pseudo code of the paper, just adds more coniditions
        Uses the closes preceding node function.
        :param id: The ID of the node that is trying to join the network
        :return:
        """
        if self.id < int(id) < self.succ_id or self.succ_id == self.id:
            return self.succ
        else:
            finger = self.closest_preceding_node(id)
            if finger == self.address:
                return self.address

            address = self.request_handler.send_message(finger, "find_successor:{}".format(id))
            if address == "error":
                return self.address
            return address

    def closest_preceding_node(self, id):
        """
        Closest preceding node function, same as the paper pseudo code. Goes through the entries in the finger table
        and if one satisfies the condition, returns the address
        :param id: ID of the node trying to join
        :return: Address of the finger table entry that satisfies the requirement
        """
        # closest = None
        # min_distance = pow(2, MAX_BITS) + 1
        # print("minimum distance: ", min_distance)
        for i in range(MAX_BITS - 1, 0, -1):
            if self.finger_table[i][1] is not None and self.id < self.finger_table[i][0] < int(id):
                # if self.pred is None or self.pred == self.address or int(self.pred_id) < int(id) < int(self.id) or \
                # (int(self.pred_id) > int(self.id) > int(id)) or (int(self.pred_id) < int(id) and int(self.id) < int(id)):
                return self.finger_table[i][1]
        return self.address

    def find_predecessor(self, id):
        """
        Find predecessor method
        :param id:
        :return:
        """
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
        """
        Join method. When a node wants to join a network, this method is called. It makes a request to the given address
        to receive what should be the node's successor. If the request fails, it prints the error message
        :param address: The known address of the network that we want to join
        :return: Does not return something
        """
        succ = self.request_handler.send_message(address,
                                                 "join:{}:{}:{}".format(self.id, self.ip, self.port))

        if succ == "error":
            print()
            print("===============================================")
            print("=============== UNABLE TO JOIN ================")
            print("=== COULD NOT CONNECT TO THE IP/PORT GIVEN ====")
            print("===============================================")
            print()
            return

        self.succ = succ
        self.succ_id = get_hash(succ[0] + ":" + str(succ[1]))
        # self.finger_table[0][0] = self.succ_id
        # self.finger_table[0][1] = self.succ
        print("Node {} successfully joined the Chord ring".format(self.id))

    def notify(self, id, ip, port):
        """
        Recevies notification from stabilize function when there is change in successor. Based on the condition, it will
        change the predessor to the ip and port number given
        :param id: The id
        :param ip: IP of predecessor
        :param port: Port number of predecessor
        :return: Empty
        """
        if self.pred is None or self.pred == self.address or int(self.pred_id) < int(id) < int(self.id) or \
                (int(self.pred_id) > int(self.id) > int(id)) or (int(self.pred_id) < int(id) and int(self.id) < int(id)):
            self.pred = (ip, int(port))
            self.pred_id = get_hash(ip + ":" + port)

    def fix_fingers(self):
        while self.run_threads:
            # index = random.randint(1, MAX_BITS-1)
            for i in range(1, MAX_BITS):
                data = self.find_successor(self.finger_table[i][0])
                # if data == None:
                #     time.sleep(SLEEP_TIME)
                #     continue
                self.finger_table[i][1] = data
                time.sleep(SLEEP_TIME)

    # def fix_fingers(self):
    #     while self.run_threads:
    #         for i in range(1, MAX_BITS):
    #             finger = self.finger_table[i][0]
    #             self.finger_table[i][1] = self.find_successor(finger)
    #         time.sleep(SLEEP_TIME)

    def stabilize(self):
        while self.run_threads:
            if self.succ is None:
                time.sleep(SLEEP_TIME)
                continue
            # if self.succ == self.address:
            #     time.sleep(SLEEP_TIME)
            #     continue
            result = self.request_handler.send_message(self.succ, "get_predecessor")

            if result == "error":
                self.succ_id = self.id
                self.succ = self.address
                # result = [self.id, self.address]
            elif result[0] is not None:
                id = get_hash(result[1][0] + ":" + str(result[1][1]))
                if int(self.id) < int(id) < int(self.succ_id) or int(self.succ_id) == int(self.id) or \
                        (int(self.succ_id) < int(self.id) and int(self.succ_id > int(id))) or \
                        (self.succ_id < int(id) and self.id < int(id)):
                    self.succ_id = id
                    self.succ = (result[1][0], result[1][1])
            self.request_handler.send_message(self.succ, "notify:{}:{}:{}".format(self.id, self.ip, self.port))
            time.sleep(SLEEP_TIME)

    def check_predecessor(self):
        while self.run_threads:
            time.sleep(SLEEP_TIME)
            if self.pred is None or self.pred == self.address:
                continue
            ping_result = self.request_handler.send_message(self.pred, "ping")
            if ping_result == "pinged":
                continue
            self.pred = None
            self.pred_id = None

    def menu(self):
        '''
        responsible for the menu of the client
        called by the start_node function
        :return:
        '''
        while self.run_threads:
            self.print_menu()
            mode = input()
            if mode == '1':
                self.print_finger_table()
                pass
            elif mode == '2':
                self.print_predecessor()
            elif mode == '3':
                self.print_successor()
            pass

    def print_menu(self):
        print("Node ID: {}".format(self.id))
        print("===============================================")
        print("==================  Menu  =====================")
        print("===============================================")
        print("""\n1. Print Finger Table
                 \n2. Print Predecessor
                 \n3. Print Successor""")

    def print_predecessor(self):
        print("Predecessor:", self.pred_id)

    def print_successor(self):
        print("Successor:", self.succ_id)

    def print_finger_table(self):
        for key, value in self.finger_table:
            print("KeyID:", key, "Value", value)


if __name__ == '__main__':

    # create chord ring
    if len(sys.argv) == 3:
        ip = sys.argv[1]
        port = int(sys.argv[2])

        node = Node(ip, port)
        print("Node launched with ID:", node.id)
        node.start()

    elif len(sys.argv) == 5:
        known_ip = sys.argv[1]
        known_port = int(sys.argv[2])

        ip = sys.argv[3]
        port = int(sys.argv[4])

        node = Node(ip, port)
        node.join((known_ip, known_port))
        print("Node launched with ID:", node.id)
        node.start()
        
    else:
        print()
        print("Not enough arguments given")
        print("Usage: python node.py <ip> <port>")
        print()
        print("Now Exiting ------------>")
        print("Goodbye")
        exit()