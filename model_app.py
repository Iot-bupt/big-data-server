from config import *

model_app = Blueprint('model_app', __name__)

@model_app.route('/get-app', methods=['GET', 'POST'])
def get_app():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        sql_select = "select * from app where 1 = 1"
        if 'tenantId' in data:
            sql_select = sql_select + " and tenant_id = %d" % int(data.get('tenantId'))
        if  'modelId' in data:
            sql_select = sql_select + " and model_id = %d" % int(data.get('modelId'))
        if  'appId' in data:
            sql_select = sql_select + " and app_id = %d" % int(data.get('appId'))
        db = mysql(**mysql_args)
        res = {'data': []}
        for i, item in enumerate(db.select(sql_select)):
            tmp = {}
            tmp['app_id'] = item[0]
            tmp['app_name'] = item[1]
            tmp['model_id'] = item[2]
            tmp['app_input'],  tmp['app_output'] = [], []
            if item[3]:
                tmp['app_input'] = json.loads(item[3])
            if item[4]:
                tmp['app_output'] = json.loads(item[4])
            tmp['tenant_id'] = item[5]
            tmp['stop'] = item[6]
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model_app.route('/create-app', methods=['GET', 'POST'])
def create_app():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        tenant_id, model_id, data_source, app_name, app_output = None, None, None, '', ''
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data.get('tenantId'))
        assert 'modelId' in data,  'missing parameters model id!'
        model_id = int(data.get('modelId'))
        assert 'dataSource' in data,  'missing parameters data source!'
        data_source = json.loads(data.get('dataSource'))
        #print(tenant_id, model_id, data_source)
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

@model_app.route('/start-app', methods=['GET', 'POST'])
def start_app():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'appId' in data, 'missing parameters app id!'
        app_id = int(data['appId'])
        timeout = int(data.get('timeout', 60))
        db = mysql(**mysql_args)
        sql_select = "select model_path from data_model, app" \
                     + " where data_model.model_id = app.model_id" \
                     + " and app_id = %d" \
                       % (app_id)
        rows = list(db.select(sql_select))
        assert len(rows) >= 1, 'no match model!'
        model_path = rows[0][0]
        sql_select = "select app_input from app" \
                     + " where app_id = %d" % (app_id)
        rows = list(db.select(sql_select))
        app_input = json.loads(rows[0][0])
        app = job(app_id, model_path, app_input, kafka_servers, timeout)
        app.start()
        #app2job.setdefault(app_id, app)
        sql_update = "update app set stop = 0 where app_id = %d" % (app_id)
        db.update(sql_update)
        db.close()
        resp = jsonify(str({'status': 'start app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model_app.route('/stop-app', methods=['GET', 'POST'])
def stop_app():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'appId' in data, 'missing parameters app id!'
        app_id = int(data['appId'])
        db = mysql(**mysql_args)
        sql_update = "update app set stop = 1 where app_id = %d" % (app_id)
        db.update(sql_update)
        db.close()
        resp = jsonify(str({'status': 'stop app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model_app.route('/delete-app', methods=['GET', 'POST'])
def delete_app():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        assert 'appId' in data, 'missing parameters app id!'
        app_id = int(data['appId'])
        db = mysql(**mysql_args)
        sql_delete = "delete from app where tenant_id = %d and app_id = %d" % (tenant_id, app_id)
        db.delete(sql_delete)
        db.close()
        resp = jsonify(str({'status': 'delete app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@model_app.route('/real-predict', methods=['GET', 'POST'])
def real_predict():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'appId' in data, 'missing parameters app id!'
        app_id = data['appId']
        consumer = KafkaConsumer(app_id,
                                 bootstrap_servers=kafka_servers,
                                 group_id='app_'+app_id+'_real_predict_'+str(time.time()))
        msg_value = list(consumer.poll(timeout_ms=5000, max_records=1).values())[0][0]\
            .value.decode('utf-8').replace('\'', '\"')
        resp = jsonify(str({'data': msg_value}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)