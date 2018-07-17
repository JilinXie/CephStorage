class Mirror(object):
    '''
    indexing keys stored in ceph for quicker searching.
    index stored on mongoDB.
    '''
    coll = None

    @classmethod
    def init_mirror(cls, mirror_coll):
        cls.coll = mirror_coll

    @classmethod
    def add_mirror(cls, bucket_name, mirror_host, origin_host):
        cls.coll.update({'mirror_host': mirror_host},
                        {'$set': {'bucket_name': bucket_name,
                                  'origin_host': origin_host}},
                        upsert=True)

    @classmethod
    def get_mirror(cls, mirror_host):
        res = cls.coll.find_one({'mirror_host': mirror_host})
        return res

    @classmethod
    def list_mirror(cls, bucket_name):
        res = cls.coll.find({'bucket_name': bucket_name})
        res = [x for x in res]
        [x.pop('_id') for x in res]
        return res

    @classmethod
    def delete_mirror(cls, mirror_host):
        cls.coll.delete_one({'mirror_host': mirror_host})
