
class Node:

    def __init__(self, id):
        self.id = id
        self.pred_id = None
        self.succ_id = None
        self.finger_table = {}
        self.storage = {}

    def create(self):
        self.pred_id = None
        # self.succ_id = n
    
    # def join(self, node):

    # def stabilize(self):

    # def notify(self, node):

    # def fix_fingers(self):

    # def check_predecessor(self):
