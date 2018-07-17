from flask import Flask
from blueprints import cephapi
from blueprints import mproxy


app = Flask('CephStorageServer')

app.register_blueprint(cephapi, url_prefix='/ceph')
app.register_blueprint(mproxy, url_prefix='/mirrors')


if __name__ == '__main__':
    print app.url_map
    app.run(debug=True)
