import logging
import gevent
import os
import requests
import time
from flask import Blueprint, jsonify, Response
from flask import request, abort, stream_with_context
from _cephutils import manager
from gevent import monkey


monkey.patch_all()
mproxy = Blueprint('mirrorproxy', 'mirrorproxy')


@mproxy.route('/host', methods=['GET', 'POST', 'DELETE'])
def mirror_op():
    if request.method == 'POST':
        try:
            mirror_host = request.json['mirror']
            origin_host = request.json['origin']
            bucket_name = request.json['bucket']
        except Exception as err:
            logging.error('Missing parameters: %s' % err)
            return jsonify({'msg': 'missing parameters'})
        if manager.add_mirror(bucket_name, mirror_host, origin_host):
            return jsonify({'msg': 'success'})
        else:
            res = jsonify({'msg': 'fail adding mirror'})
            res.status_code = 500
            return res
    if request.method == 'GET':
        try:
            bucket_name = request.args['bucket_name']
        except Exception as err:
            logging.error('Missing parameters: %s' % err)
            return jsonify({'msg': 'missing parameters'})
        mirrors = manager.list_mirror(bucket_name)
        return jsonify({'mirrors': mirrors})
    if request.method == 'DELETE':
        try:
            mirror_host = request.args['mirror_host']
        except Exception as err:
            logging.error('Missing parameters: %s' % err)
            return jsonify({'msg': 'missing parameters'})
        manager.delete_mirror(mirror_host)
        return jsonify({'msg': 'success'})


def download_and_ceph(generator, filepath, bucket_name, key_name, key_url):
    with open(filepath, 'a') as f:
        while True:
            chunk = generator.next()
            if not chunk:
                break
            f.write(chunk)
    manager.create_key_from_local(filepath, key_name, key_url, bucket_name)


def sync_key_range(url, range_header):
    chunk_size = 4*1024
    try:
        res = requests.get(url, headers={'Range': range_header}, stream=True)
    except Exception as err:
        logging.error('error request: %s: %s' % (url, err))
    return res.iter_content(chunk_size=chunk_size), res.headers['content-type']


def sync_key(url, bucket_name, key_name):
    '''
    download from url, stream to user while uploading to Ceph
    If user connection close, will go on uploading process in gevent.
    '''
    try:
        res = requests.get(url, stream=True)
    except Exception as err:
        logging.error('error request: %s: %s' % (url, err))
        return None, None
    chunk_size = 4*1024
    if not bucket_name:
        return (res.iter_content(chunk_size=chunk_size),
                res.headers['content-type'])

    def generate():
        filepath = '%s_%s_%d' % (bucket_name, key_name, int(time.time()))
        filepath = filepath.replace('/', '_')
        try:
            chunk_written = 0
            generator = res.iter_content(chunk_size=chunk_size)
            with open(filepath, 'w') as f:
                while True:
                    chunk = generator.next()
                    if not chunk:
                        break
                    f.write(chunk)
                    chunk_written += chunk_size
                    yield chunk
        except Exception as err:
            logging.error(err)
        finally:
            gevent.spawn(download_and_ceph, generator, filepath,
                         bucket_name, key_name, url)

    return generate(), res.headers['content-type']


@mproxy.route('/proxy/<path:url>', methods=['GET'])
def mirror_proxy(url):
    mirror_host = request.headers['host']
    range_h = request.headers.get('Range', '')
    res = manager.get_mirror(mirror_host)
    try:
        bucket_name = res['bucket_name']
        origin_host = res['origin_host']
    except Exception as err:
        logging.error('mirror not found: %s' % err)
        abort(400)
    key_name = url
    key = manager.get_key(bucket_name, key_name)

    if key and key.get('url', ''):
        stream, ct = sync_key_range(key['url'], range_h)
    else:
        url = os.path.join(origin_host, url)
        if not url.startswith('http'):
            url = 'http://' + url

        if not range_h:
            stream, ct = sync_key(url, bucket_name, key_name)
        else:
            stream, ct = sync_key_range(url, range_h)
            gevent.spawn(manager.create_key_from_remote, key_name,
                         url, bucket_name)
    return Response(stream_with_context(stream), content_type=ct)
