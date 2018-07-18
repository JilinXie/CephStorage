from pymongo import MongoClient


mongo_client = MongoClient('mongodb://127.0.0.1:27017')
# collection storing progress.
progress_coll = mongo_client.CephStorageGeneral.progress
index_db = mongo_client.CephStorageIndex
general_db = mongo_client.CephStorageGeneral

'''
DBs and Collections used:
    CephStorageIndex: Store key indexes.
    CephStorageGeneral: General Info
    CephStorageConfiguratoin: Config Info
    CephStorageMonitor: Monitoring Data
'''


class Configuration(object):
    '''
    CephStorageConfiguration:
    {
        'access_key': String,
        'secret_key': String,
        'rgw_address': String,
        'download_path': String,
        'public_proxy': String,
        'index_db': String,
    }
    '''

    def __init__(self):
        self.mc = mongo_client
        self.coll = mongo_client.CephStorageConfiguration.config
        self.modify_data = {}
        self.config = None

    def update_credentials(self, ak, sk):
        self.modify_data = {'access_key': ak, 'secret_key': sk}

    def update_download_path(self, path):
        self.modify_data['download_path'] = path

    def update_public_proxy(self, proxy):
        self.modify_data['public_proxy'] = proxy

    def update_rgw_address(self, rgw_host):
        self.modify_data['rgw_address'] = rgw_host

    def update(self):
        if self.modify_data:
            self.coll.update({'config': 'general'},
                             {'$set': self.modify_data},
                             upsert=True)

    def load(self):
        self.config = self.coll.find_one()

    def get_credentials(self):
        return self.config['access_key'], self.config['secret_key']

    def get_download_path(self):
        return self.config['download_path']

    def get_public_proxy(self):
        return self.config['public_proxy']

    def get_rgw_address(self):
        return self.config['rgw_address']
