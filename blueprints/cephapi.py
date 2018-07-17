import logging
from _cephutils import manager
from flask import Blueprint, jsonify, request


cephapi = Blueprint('cephapi', 'cephapi')


@cephapi.route('/<username>/<bucketname>/keys',
               defaults={'keyname': ''})
@cephapi.route('/<username>/<bucketname>/keys/<keyname>',
               methods=['GET', 'POST', 'DELETE'])
def keys(username, bucketname, keyname):
    bucket_name = '%s___%s' % (username, bucketname)
    if request.method == 'GET':
        if not keyname:
            keys = manager.list_key(bucket_name)
            return jsonify({'keys': keys})
        key = manager.get_key(bucket_name, keyname)
        return jsonify(key)
    if request.method == 'POST':
        try:
            key_url = request.json['key_url']
            key_name = request.json['key_name']
            content_type = request.json.get('content_type', '')
            md5 = request.json.get('md5', '')
            timetag = request.json.get('timetag', 0)
        except Exception as err:
            logging.error('invalid parameters: %s' % err)
            return jsonify({'msg': 'missing required parameters'})
        token = manager.create_key_from_remote(key_name, key_url, bucket_name,
                                               content_type, md5, timetag)
        return jsonify({'token': token})
    if request.method == 'DELETE':
        if not keyname:
            logging.error('invalid parameters: %s' % err)
            return jsonify({'msg': 'missing required parameters'})
        ok = manager.delete_key(bucket_name, keyname)
        if ok:
            res = jsonify({'msg': 'key_deleted'})
            res.status_code = 200
        else:
            res = jsonify({'msg': 'error deleting key'})
            res.status_code = 500
        return res


@cephapi.route('/progress/<token>', methods=['GET'])
def progress(token):
    state = manager.query_key_state(token)
    if not state:
        res = jsonify({'msg': 'task not found'})
        res.status_code = 404
        return res
    res = jsonify({'state': state})
    return res
