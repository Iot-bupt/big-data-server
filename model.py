from config import *

model = Blueprint('model', __name__)

@model.route('/get-general-model', methods=['GET', 'POST'])
def get_general_model():
    try:
        sql_select = "select * from data_model where tenant_id = -1"
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        if 'modelId' in data:
            sql_select = sql_select + " and model_id = %d" % int(data.get('modelId'))
        db = mysql(**mysql_args)
        res = {'data':[]}
        for i, item in enumerate(db.select(sql_select)):
            tmp = {}
            tmp['model_id'] = item[0]
            tmp['model_name'] = item[1]
            tmp['model_desc'] = item[2]
            tmp['model_input'] = json.loads(item[3])
            tmp['model_path'] = item[4]
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model.route('/get-tenant-model', methods=['GET', 'POST'])
def get_tenant_model():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        sql_select = "select * from data_model where tenant_id = %d" % tenant_id
        if 'modelId' in data:
            sql_select = sql_select + " and model_id = %d" % int(data.get('modelId'))
        db = mysql(**mysql_args)
        res = {'data':[]}
        for i, item in enumerate(db.select(sql_select)):
            tmp = {}
            tmp['model_id'] = item[0]
            tmp['model_name'] = item[1]
            tmp['model_desc'] = item[2]
            tmp['model_input'] = json.loads(item[3])
            tmp['model_path'] = item[4]
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model.route('/create-model', methods=['GET','POST'])
def create_model():
    assert request.method == 'POST', 'method must be post!'
    model_id = ''
    try:
        assert 'model_file' in request.files, 'no model file!'
        model_file = request.files['model_file']
        data = request.form
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = data['tenantId']
        if not os.path.isdir(model_path):

            os.makedirs(model_path)
        if not os.path.isdir(model_path+'/'+tenant_id):
            os.makedirs(model_path+'/'+tenant_id)
        model_file_name = model_path+'/'+tenant_id + '/' + model_id + '.pkl'
        model_file.save(model_file_name)
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model.route('/delete-model', methods=['GET', 'POST'])
def delete_model():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        assert 'modelId' in data, 'missing parameters model id!'
        model_id = int(data['modelId'])
        sql_delete = "select * from data_model where tenant_id = %d and model_id = %d" % (tenant_id, model_id)
        db = mysql(**mysql_args)
        db.delete(sql_delete)
        db.close()
        resp = jsonify(str({'status': 'delete model success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)