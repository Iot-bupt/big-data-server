from config import *

hdfs = Blueprint('hdfs', __name__)


@hdfs.route('/getFiles', methods=['GET'])
def getFiles():
    try:
        data = []
        path =request.values.get('path')
        owner = request.values.get('owner')
        group = request.values.get('group')
        f = file()
        data = f.getFiles(path,owner,group)
        res = json.dumps(data, ensure_ascii=False)
        resp = Response(res, mimetype='application/json')
        #resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        # resp.headers['content_type'] = 'application/json'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@hdfs.route('/deleteFiles', methods=['GET'])
def deleteFiles():
    try:
        data = {}
        path =request.values.get('path')
        f = file()
        data = f.deleteFiles(path)
        res = json.dumps(data, ensure_ascii=False)
        resp = Response(res, mimetype='application/json')
        # resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@hdfs.route('/getFiletree', methods=['GET'])
def getFiletree():
    try:
        data = {}
        f = file()
        data = f.filetree('/')
        res = json.dumps(data, ensure_ascii=False)
        resp = Response(res, mimetype='application/json')
        # resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)
