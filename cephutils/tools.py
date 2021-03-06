import hashlib
import time


class StateRecorder(object):

    coll = None

    @classmethod
    def init_recorder(cls, progress_coll):
        cls.coll = progress_coll
        cls.coll.ensure_index("updated_at", expireAfterSeconds=3*3600)

    def __init__(self, token_seed):
        m = hashlib.md5()
        m.update(token_seed)
        self.token = m.hexdigest()

    def update_progress(self, state):
        self.coll.update({'token': self.token},
                         {'$set': {'progress': state,
                                   'updated_at': int(time.time())}},
                         upsert=True)

    def under_process(self, token):
        res = self.coll.find_one({'token': token})
        if res and res['progress'] == 'inprogress':
            return True
        return False

    def start(self):
        self.update_progress('inprogress')

    def fail(self):
        self.update_progress('fail')

    def succeed(self):
        self.update_progress('succeed')

    def invalid(self):
        self.update_progress('invalid')

    @classmethod
    def get_progress(cls, token):
        res = cls.coll.find_one({'token': token})
        if res:
            return res['progress']
        return ''
