from config import *
from db.getNosqlInfo import NoSQLTool

sql = Blueprint('sql', __name__)


@sql.route('/exec-sql', methods=['GET'])
def exec_sql():
    try:
        data = {}
        data = request.args
        print(data)
        assert 'sql' in data, 'missing parameters sql!'
        sql_string = (data['sql'])
        db = mysql()
        exec_result = db.sql_exec(sql_string)
        str_json = exec_result.to_json(orient='records')
        res = json.loads(str_json)
        db.close()
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)


@sql.route('/tables', methods=['GET'])
def get_tables():
    try:
        db = mysql()
        res = list(db.get_tables())
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@sql.route('/getTables', methods=['GET'])
def get_columns():
    try:
        db = NoSQLTool()
        res = list(db.get_columns())
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)