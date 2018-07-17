import hashlib
import logging
import os
import time
import requests


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
        m = hashlib.md5()
        m.update(key_url)
        self.token = m.hexdigest()
        self.key_url = key_url
        self.recorder = state_recorder

    '''
    The file is left for the caller to clean
    RETURN: (state, filepath)
        filepath: the file path where content is downloaded,
                  None if download fail to start or duplicate.
        state: 0 - download success
               1 - download fail
               2 - download duplicate
    '''
    def download(self):
        if self.recorder.under_process(self.token):
            return 2, None
        fp = os.path.join(self.download_path,
                          '%s_%f.tmp' % (self.token, time.time()))

        try:
            res = requests.get(self.key_url, stream=True, timeout=60)
            if res.status_code < 200 or res.status_code >= 300:
                logging.error('error request, invalid status_code: %d'
                              % res.status_code)
                self.recorder.fail()
                return 1, None
        except Exception as err:
            logging.error('error request key_url: %s; %s' % (
                          self.key_url, err))
            self.recorder.fail()
            return 1, fp

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
            return 1, fp

        return 0, fp
