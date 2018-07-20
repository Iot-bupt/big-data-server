from config import *

device = Blueprint('device', __name__)

@device.route('/device-types', methods=['GET', 'POST'])
def get_device_types():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        db = mysql(**mysql_args)
        sql_select = "select distinct(device_type)" \
                     + " from recent_data where tenant_id = %d" % (tenant_id)
        res = {'deviceTypes':[]}
        for item in db.select(sql_select):
            res['deviceTypes'].append(item[0])
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)