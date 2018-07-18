import logging
import time


class BucketIndex(object):
    index_coll = None

    @classmethod
    def init_index(cls, index_coll):
        cls.index_coll = index_coll

    @classmethod
    def add_key(cls, bucket_name, key_size):
        cls.index_coll.update({'bucket': bucket_name},
                              {'$inc': {'size': key_size,
                                        'keyamount': 1}},
                              upsert=True)

    @classmethod
    def get(cls, bucket_name):
        res = cls.index_coll.find_one({'bucket': bucket_name})
        return res


class KeyIndex(object):
    '''
    indexing keys stored in ceph for quicker searching.
    index stored on mongoDB.
    '''
    index_db = None

    @classmethod
    def init_index(cls, index_db):
        cls.index_db = index_db

    @classmethod
    def index_key(cls, bucket_name, key_name, content_type='',
                  md5='', upload_time=0, size=0):
        coll = cls.index_db[bucket_name]
        vfolder = '/'.join(key_name.split('/')[:-1])
        if not upload_time:
            upload_time = int(time.time())

        try:
            coll.update({'key': key_name},
                        {'$set': {'ts': upload_time,
                                  'vfolder': vfolder,
                                  'md5': md5,
                                  'content_type': content_type,
                                  'size': size}},
                        upsert=True)
            BucketIndex.add_key(bucket_name, size)
        except Exception as err:
            logging.error('Fail to index key on mongo: %s' % err)
            return False
        return True

    @classmethod
    def delete_key_index(cls, bucket_name, key_name):
        try:
            coll = cls.index_db[bucket_name]
            coll.delete_one({'key': key_name})
        except Exception as err:
            logging.error('error deleting keyindex: %s-%s: %s' % (
                bucket_name, key_name, err))
            return False
        return True

    @classmethod
    def list_key(cls, bucket_name, vfolder='', maximum=1000):
        coll = cls.index_db[bucket_name]
        res = coll.find({'vfolder': vfolder})
        keys = []
        for i in res:
            i.pop('_id')
            keys.append(i)
            if len(keys) >= maximum:
                break

        return keys

    @classmethod
    def get_key(cls, bucket_name, key_name):
        coll = cls.index_db[bucket_name]
        key = coll.find_one({'key': key_name})
        if key:
            key.pop('_id')
            return key
        return None
