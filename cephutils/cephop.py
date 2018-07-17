import boto
import logging
from boto.s3.connection import OrdinaryCallingFormat
from expiringdict import ExpiringDict


class CephModel(object):
    s3 = None
    bucket_cache = ExpiringDict(max_len=100, max_age_seconds=300)

    @classmethod
    def generate_url(cls, bucket_name, key_name, expire=3600*6):
        try:
            key_url = cls.s3.generate_url(expire, 'GET', bucket_name, key_name)
        except Exception as err:
            logging.error('error generating url for %s, %s; %s' %
                          bucket_name, key_name, err)
            return ''
        return key_url

    # ----------- Assisting Method -----------
    @classmethod
    def get_bucket(cls, bucket_name):
        if bucket_name in cls.bucket_cache:
            return cls.bucket_cache[bucket_name]
        bucket = cls.s3.get_bucket(bucket_name)
        cls.bucket_cache[bucket_name] = bucket
        return bucket

    @classmethod
    def init_boto(cls, access_key, secret_key, server_host):
        cls.s3 = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host=server_host,
            is_secure=False,
            calling_format=OrdinaryCallingFormat())

    # --------- Bucket Method ------------
    @classmethod
    def list_buckets(cls, prefix=''):
        bs = cls.s3.get_all_buckets()
        return [b.name for b in bs if b.name.startswith(prefix)]

    @classmethod
    def create_bucket(cls, bucket_name):
        cls.s3.create_bucket(bucket_name)

    @classmethod
    def delete_bucket(cls, bucket_name):
        cls.s3.delete_bucket(bucket_name)

    # ---------------- Key Method --------------

    # list at most 1000 keys starting at <marker>
    # next_marker is used for retrieving next bunch of keys.
    # if next_marker is empty. No more need to be retrieved.
    @classmethod
    def list_keys(cls, bucket_name, max_keys=1000, prefix='', marker=''):
        bucket = cls.get_bucket(bucket_name)
        rs = bucket.get_all_keys(prefix=prefix, marker=marker,
                                 max_keys=max_keys)

        keys = []
        for key in rs:
            name = key.name
            size = key.size
            md5 = key.etag[1:-1]
            content_type = key.content_type
            last_modified = key.last_modified
            keys.append((name, size, last_modified, md5, content_type))

        next_marker = ''
        if rs.is_truncated:
            next_marker = rs.next_marker or key.name
        return (keys, next_marker)

    @classmethod
    def modify_key_contenttype(cls, bucket_name, key_name, content_type):
        bucket = cls.get_bucket(bucket_name)
        key = bucket.get_key(key_name)
        if key is None:
            return False
        key.copy(key.bucket, key.name, preserve_acl=True,
                 metadata={'Content-Type': content_type})
        return True

    @classmethod
    def create_key(cls, bucket_name, key_name, fpath, content_type='', md5=''):
        bucket = cls.get_bucket(bucket_name)
        key = bucket.new_key(key_name)

        headers = {}
        if content_type:
            headers = {'Content-Type': content_type}

        key.set_contents_from_filename(fpath, headers=headers)
        if md5 and key.etag[1:-1] != md5:
            key.delete()
            return False, None

        return True, key

    @classmethod
    def get_key(cls, bucket_name, key_name):
        bucket = cls.get_bucket(bucket_name)

        key = bucket.get_key(key_name)
        if key is None:
            return None
        key_url = key.generate_url(7200)
        name = key.name or ''
        size = key.size or ''
        md5 = (key.etag or [])[1:-1]
        content_type = key.content_type or ''
        last_modified = key.last_modified or ''
        return {'name': name, 'size': size, 'md5': md5,
                'last_modified': last_modified,
                'content_type': content_type, 'url': key_url}

    @classmethod
    def delete_key(cls, bucket_name, key_name):
        try:
            bucket = cls.get_bucket(bucket_name)
            bucket.delete_key(key_name)
        except Exception as err:
            logging.error('error deleting key from ceph: %s-%s: %s' % (
                bucket_name, key_name, err))
            return False
        return True


if __name__ == '__main__':
    aws_access_key = 'J7ZXLB8DR39XC75I8EPE'
    aws_secret_key = 'MnJlPLhNAmsKSuzpN8JkKSc3C07N82QX41UB9kW4'
    # 202.77.21.218 hk;  120.133.6.24:bj
    rgw_host = '120.133.6.23:9999'
    CephModel.init_boto(aws_access_key, aws_secret_key, rgw_host)
    print CephModel.list_buckets()
