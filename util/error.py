from flask import jsonify

def get_error_resp(e, trans2str=False):
    if trans2str:
        resp = jsonify(str({'status': e.args[0]}))
    else:
        resp = jsonify({'status': e.args[0]})
    # 跨域设置
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
