import socket, pickle

class RequestHandler:
    
    def __init__(self):
        pass
    
    def send_message(self, address, message):
        ping = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
  
        # connect to server on local computer 
        ping.connect(address) 
        ping.send(pickle.dumps(message))
        try: 
            data = pickle.loads(ping.recv(1024))
            return data
        except:
            pass
        ping.close()