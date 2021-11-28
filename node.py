
class Node:

    def __init__(self, id):
        '''
        storage:
        '''
        self.id = id
        self.pred_id = None
        self.succ_id = None
        self.finger_table = {}
        self.storage = {}

    def create(self):
        '''
        
        '''
        pass
        # self.pred_id = None
        # self.succ_id = n
    
    def join(self, node):
        '''

        '''
        pass

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
