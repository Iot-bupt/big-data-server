from config import *

model = Blueprint('model', __name__)

def get_data(engine, source_table, feature_columns, target_columns, test_size=0.0):
    data = pd.read_sql(source_table, engine)
    X_train, X_test, y_train, y_test = train_test_split(
        data[feature_columns], data[target_columns], test_size=test_size)
    return  X_train, X_test, y_train, y_test

def train(data_model, x, y):
    data_model.fit(x, y)

def define_model(model_type):
    if model_type == 'reg':
        return LinearRegression()
    if model_type == 'cla':
        return LogisticRegression()
    return None

def save_model(data_model, model_file_name, model_file_path=model_path):
    if not os.path.isdir(model_file_path):
        os.makedirs(model_file_path)
    joblib.dump(data_model, model_file_path + '/' + model_file_name + '.pkl')

@model.route('/get-model-id', methods=['GET', 'POST'])
def get_model_id():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        sql_select = "select * from data_model where tenant_id = -1 or tenant_id = %d" % tenant_id
        db = mysql(**mysql_args)
        res = {'data':[]}
        for i, item in enumerate(db.select(sql_select)):
            tmp = {}
            tmp['model_id'] = item[0]
            tmp['model_name'] = item[1]
            # tmp['model_desc'] = item[2]
            # tmp['model_input'] = json.loads(item[3])
            # tmp['model_state'] = item[4]
            # tmp['model_path'] = item[5]
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

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
        resp = jsonify(res)
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
            tmp['model_state'] = item[4]
            tmp['model_path'] = item[5]
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(res)
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
        tenant_id = int(data['tenantId'])
        model_id = int(time.time())
        assert 'input' in data, 'missing parameters model input!'
        model_input = data['input']
        model_name = data.get('modelName', str(model_id))
        model_desc = data.get('modelDesc', str(model_id))
        model_state = 0
        for item in json.loads(model_input):
            assert isinstance(item, str), 'model input parameter format wrong!'
        sql_insert = "insert into data_model(model_id, model_name, model_desc, model_input, model_state, tenant_id)" \
                     + " values(%d, '%s', '%s', '%s', %d, %d)" \
                      % (model_id, model_name ,model_desc, model_input, model_state, tenant_id)
        db = mysql(**mysql_args)
        db.insert(sql_insert)
        db.close()
        resp = jsonify({'model id': model_id, 'status': 'create model success, next you need train model!'})
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

        db_con_args = mysql_args
        if 'host' in data and 'user' in data and 'passwd' in data and 'dbname' in data:
            db_con_args['host'] = data['host']
            db_con_args['user'] = data['user']
            db_con_args['passwd'] = data['passwd']
            db_con_args['dbname'] = data['dbname']
            db_con_args['port'] = int(data.get('port', 3306))
        engine = get_mysql_engine(db_con_args)

        assert 'sourceTable' in data, 'missing parameters source table!'
        source_table = data['sourceTable']

        assert 'featureColumns' in data, 'missing parameters feature columns!'
        feature_columns = json.loads(data['featureColumns'])
        for item in feature_columns:
            assert isinstance(item, str), 'feature columns parameter format wrong!'

        assert 'targetColumns' in data, 'missing parameters target columns!'
        target_columns = json.loads(data['targetColumns'])
        for item in target_columns:
            assert isinstance(item, str), 'feature columns parameter format wrong!'

        sql_select = "select model_input from data_model where model_id = %d" % (model_id)
        db = mysql(**mysql_args)
        rows = list(db.select(sql_select))
        assert len(rows) >= 1, 'no match model!'
        model_input = json.loads(rows[0][0])
        assert len(feature_columns) == len(model_input), 'feature columns do not match model input!'

        assert  'modelType' in data, 'missing parameters model type!'
        model_type = data['modelType']
        assert model_type in ['reg', 'cla'], 'model type must 1. linear regression or 2. logistic regression for classify!'

        data_model = define_model(model_type)
        x, _, y, _ = get_data(engine, source_table, feature_columns, target_columns)
        train(data_model, x, y)
        model_file_name = str(model_id)
        model_file_path = model_path
        save_model(data_model, model_file_name=model_file_name, model_file_path=model_file_path)

        sql_update = "update data_model set model_path = '%s' where model_id = %d" \
                     % ( model_file_path + '/' + model_file_name + '.pkl', model_id)
        db.update(sql_update)
        sql_update = "update data_model set model_state = 1 where model_id = %d" % (model_id)
        db.update(sql_update)
        db.close()
        resp = jsonify({'model id':model_id, 'status': 'train model success!'})
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
        # assert 'tenantId' in data, 'missing parameters tenant id!'
        # tenant_id = int(data['tenantId'])
        assert 'modelId' in data, 'missing parameters model id!'
        model_id = int(data['modelId'])
        #sql_delete = "delete from data_model where tenant_id = %d and model_id = %d" % (tenant_id, model_id)
        db = mysql(**mysql_args)
        sql_select = "select * from app where model_id = %d" % (model_id)
        if len(list(db.select(sql_select))) > 0:
            resp = jsonify({'model id': model_id, 'status': 'delete model failed, there is app crerated from this model!'})
            resp.headers['Access-Control-Allow-Origin'] = '*'
            return resp
        sql_delete = "delete from data_model where model_id = %d" % (model_id)
        db.delete(sql_delete)
        db.close()
        resp = jsonify({'model id':model_id, 'status': 'delete model success!'})
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)