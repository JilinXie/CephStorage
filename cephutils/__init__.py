import gevent
import logging
import os
import time
from gevent import monkey
from index import KeyIndex
from cephop import CephModel
from upload import Downloader
from tools import StateRecorder
from mirror import Mirror

monkey.patch_all()


class StorageManager(object):

    '''
    ak: access_key to ceph-s3 service.
    sk: secret_key
    public_proxy: public proxy used for clients outside lan ceph deployed.
    progress_coll: db where downloading progress recorded.
    download_path: downloading path where key content is temporarily stored.
    index_db: db where index are stored.
    '''
    def __init__(self, ak, sk, server_host, public_proxy,
                 progress_coll, download_path,
                 index_db, mirror_db):
        CephModel.init_boto(ak, sk, server_host)
        Downloader.init_downloader(download_path)
        StateRecorder.init_recorder(progress_coll)
        KeyIndex.init_index(index_db)
        Mirror.init_mirror(mirror_db)
        self.public_proxy = public_proxy

    def _create_key_from_local(self, filepath, recorder, key_name,
                               bucket_name, content_type, md5, timetag):
        recorder.start()
        try:
            # --- upload to ceph ----
            success, key = CephModel.create_key(bucket_name, key_name,
                                                filepath, content_type, md5)
            if md5 and not key and not success:
                recorder.invalid()
                return
            if not success:
                recorder.fail()
                return

            # --- tag index ----
            if not content_type:
                content_type = key.content_type
            md5 = key.etag[1:-1]

            if not timetag:
                timetag = int(time.time())

            success = KeyIndex.index_key(bucket_name, key_name, content_type,
                                         md5, timetag, key.size)
            if not success:
                recorder.fail()
                return

            recorder.succeed()
        except Exception as err:
            logging.error('error create key: %s, %s, %s' % (
                bucket_name, key_name, err))
        finally:
            if filepath:
                os.remove(filepath)

    def create_key_from_local(self, key_name, key_url, bucket_name,
                              content_type='', md5='', timetag=0):
        recorder = StateRecorder(key_url+key_name+bucket_name)
        downloader = Downloader(key_url, recorder)
        gevent.spawn(self._create_key_from_remote, downloader, recorder,
                     key_name, bucket_name, content_type, md5, timetag)

        return recorder.token

    def _create_key_from_remote(self, downloader, recorder, key_name,
                                bucket_name, content_type, md5, timetag):
        recorder.start()
        try:
            # --- download ----
            state, fpath = downloader.download()
            if state == 1:
                recorder.fail()
                return
            elif state == 2:
                return

            # --- upload to ceph ----
            success, key = CephModel.create_key(bucket_name, key_name,
                                                fpath, content_type, md5)
            if md5 and not key and not success:
                recorder.invalid()
                return
            if not success:
                recorder.fail()
                return

            # --- tag index ----
            if not content_type:
                content_type = key.content_type
            md5 = key.etag[1:-1]

            if not timetag:
                timetag = int(time.time())

            success = KeyIndex.index_key(bucket_name, key_name,
                                         content_type, md5,
                                         timetag, key.size)
            if not success:
                recorder.fail()
                return

            recorder.succeed()
        except Exception as err:
            logging.error('error create key: %s, %s, %s' % (
                bucket_name, key_name, err))
        finally:
            if fpath:
                os.remove(fpath)

    #    --------------------- Key Operation ------------------
    '''
    - key_nam: key's name set on Ceph
    - key_url: url to download key content
    - bucket_name: bucket of Ceph to store key
    - content_type: custom content_type for key [OPTIONAL]
    - md5: md5 value of key, will be compared with key's md5 after download
           to check the integrity [OPTIONAL]
    - timetag: a timestamp recording when the key is added.
               [DEFAULT]: current time{time.time()}
    '''
    def create_key_from_remote(self, key_name, key_url, bucket_name,
                               content_type='', md5='', timetag=0):
        recorder = StateRecorder(key_url+key_name+bucket_name)
        downloader = Downloader(key_url, recorder)
        gevent.spawn(self._create_key_from_remote, downloader, recorder,
                     key_name, bucket_name, content_type, md5, timetag)

        return recorder.token

    def query_key_state(self, token):
        return StateRecorder.get_progress(token)

    '''
    RETURN: {
        name: String,
        size: Int,
        md5: String,
        last_modified: Date,
        content_type: String,
        url: String
    }
    '''

    '''
    key: String
    RETURN: key deleted.
    '''
    def delete_key(self, bucket_name, key):
        ok = KeyIndex.delete_key_index(bucket_name, key)
        ok = ok and CephModel.delete_key(bucket_name, key)

        return ok

    def list_key(self, bucket_name, vfolder=''):
        keys = KeyIndex.list_key(bucket_name, vfolder)
        return keys

    def get_key(self, bucket_name, key_name):
        key = KeyIndex.get_key(bucket_name, key_name)
        if not key:
            return {}
        key_url = CephModel.generate_url(bucket_name, key_name)
        key_url = key_url.split('/')
        key_url[2] = self.public_proxy
        key_url = '/'.join(key_url)

        key['url'] = key_url

        return key

    # ----------------------------- Bucket Operation -------------------
    def add_mirror(self, bucket_name, mirror_host, origin_host):
        try:
            Mirror.add_mirror(bucket_name, mirror_host, origin_host)
            return True
        except Exception as err:
            logging('error adding mirror: %s, %s, %s: %s' % (
                mirror_host, bucket_name, origin_host, err))
            return False

    def list_mirror(self, bucket_name):
        return Mirror.list_mirror(bucket_name)

    def delete_mirror(self, mirror_host):
        Mirror.delete_mirror(mirror_host)

    def get_mirror(self, mirror_host):
        return Mirror.get_mirror(mirror_host)
