import logging
import os
import random
import string
import time
import requests
from string import ascii_uppercase, digits


class Downloader(object):

    '''
    Record downloading progress in mongoDB.
    '''
    download_path = '/tmp'
    chunk_size = 8*1024

    @classmethod
    def init_downloader(cls, download_path='/tmp'):
        cls.download_path = download_path

    def __init__(self, key_url, state_recorder):
        t = ''.join(random.choice(ascii_uppercase + digits) for _ in range(10))

        self.token = '%s.%.3f' % (t, time.time())
        self.key_url = key_url
        self.recorder = state_recorder

    '''
    The file is left for the caller to clean
    '''
    def download(self):
        rs = ''.join(random.choice(string.ascii_uppercase + string.digits)
                     for _ in range(15))
        fp = os.path.join(self.download_path,
                          '%s_%f.tmp' % (rs, time.time()))

        try:
            res = requests.get(self.key_url, stream=True, timeout=60)
            if res.status_code < 200 or res.status_code >= 300:
                logging.error('error request, invalid status_code: %d'
                              % res.status_code)
                self.recorder.fail()
                return False, None
        except Exception as err:
            logging.error('error request key_url: %s; %s' % (
                          self.key_url, err))
            self.recorder.fail()
            return False, fp

        try:
            with open(fp, 'wb') as f:
                for chunk in res.iter_content(chunk_size=self.chunk_size):
                    if not chunk:
                        break
                    f.write(chunk)
        except Exception as err:
            logging.error('error of %s writing to dist: %s' % (
                          self.key_name, err))
            self.recorder.fail()
            return False, fp

        return True, fp
