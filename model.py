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
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = data['tenantId']
        model_id = int(time.time())
        assert 'input' in data, 'missing parameters model input!'
        model_input = data['input']
        model_name = data.get('modelName', '')
        model_desc = data.get('modelDesc', '')
        model_state = 0
        for item in json.loads(model_input):
            assert isinstance(item, str), 'model inpit parameter format wrong!'
        sql_insert = "insert into data_model(model_id, model_name, model_desc, model_input, model_state, tenant_id)" \
                     + " values(%d, '%s', '%s', '%s', %d, %d)" \
                      % (model_id, model_name ,model_desc, model_input, model_state, tenant_id)
        db = mysql(**mysql_args)
        db.insert(sql_insert)
        db.close()
        resp = jsonify(str({'status': 'create model success, next you need train model!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
        # if not os.path.isdir(model_path):
        #     os.makedirs(model_path)
        # if not os.path.isdir(model_path+'/'+tenant_id):
        #     os.makedirs(model_path+'/'+tenant_id)
        # model_file_name = model_path+'/'+tenant_id + '/' + model_id + '.pkl'
        # model_file.save(model_file_name)
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model.route('/train-model', methods=['GET', 'POST'])
def train_model():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'modelId' in data, 'missing parameters model id!'
        model_id = int(data.get('modelId'))
        assert 'sourceTable' in data, 'missing parameters source table!'
        source_table = data['sourceTable']
        assert 'featureColumns' in data, 'missing parameters feature columns!'
        feature_columns = json.loads(data['featureColumns'])
        assert 'targetColumns' in data, 'missing parameters target columns!'
        target_columns = json.loads(data['targetColumns'])

        sql_select = "select model_input from data_model where model_id = %d" % (model_id)
        db = mysql(**mysql_args)
        rows = list(db.select(sql_select))
        assert len(rows) >= 1, 'no match model!'
        model_input = json.loads(rows[0][0])
        app_input = []
        #print(model_input)
        assert len(data_source) == len(model_input), 'data source do not match model input!'
        for device_id, data_type in zip(data_source, model_input):
            app_input.append({"device_id":device_id, "type":data_type})
        app_id = int(time.time())
        app_input = json.dumps(app_input)
        sql_insert = "insert into app(app_id, app_name, model_id, app_input, app_output, tenant_id, stop)" \
                     + " values(%d, '%s', %d, '%s', '%s', %d, %d)" \
                      % (app_id, app_name, model_id, app_input, app_output, tenant_id, 1)
        #print(sql_insert)
        db.insert(sql_insert)
        db.close()
        resp = jsonify(str({'status': 'create app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
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
        sql_delete = "delete from data_model where tenant_id = %d and model_id = %d" % (tenant_id, model_id)
        db = mysql(**mysql_args)
        db.delete(sql_delete)
        db.close()
        resp = jsonify(str({'status': 'delete model success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)