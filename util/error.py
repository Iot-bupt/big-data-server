from flask import jsonify

def get_error_resp(e):
    resp = jsonify(str({'status': e.args[0]}))
    # 跨域设置
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
