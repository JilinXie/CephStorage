import sys
try:
    from config import Configuration, progress_coll, index_db, mirror_coll
    from cephutils import StorageManager
except Exception:
    sys.path.append('..')
    from config import Configuration, progress_coll, index_db, mirror_coll
    from cephutils import StorageManager


conf = Configuration()
conf.load()
ak, sk = conf.get_credentials()
rgw_address = conf.get_rgw_address()
public_proxy = conf.get_public_proxy()
download_path = conf.get_download_path()


manager = StorageManager(ak, sk, rgw_address, public_proxy, progress_coll,
                         download_path, index_db, mirror_coll)
