from config import *
from analysis import analysis
from model import model
from model_app import model_app
from device import device

app = Flask(__name__)

app.register_blueprint(analysis, url_prefix='/api/analysis')
app.register_blueprint(model, url_prefix='/api/model')
app.register_blueprint(model_app, url_prefix='/api/app')
app.register_blueprint(device, url_prefix='/api/device')

if __name__ == '__main__':
    app.run(port=8092, host='0.0.0.0',debug=True)

########################################################
"""
import subprocess
from flask import Flask, request, jsonify
from util.mysql import *
from util.job import *

kafka_servers = ['172.30.26.6:9092']
mysql_args = {'host':'172.24.32.169', 'user':'root', 'passwd':'root', 'dbname':'BUPT_IOT'}
#app2job = {}

app = Flask(__name__)


def get_resp(args, time_out = 90 * 1000, tenant_id = '-1', topic_type = 'data'):
    consumer = KafkaConsumer(str(tenant_id + '_' + topic_type),
                             bootstrap_servers=kafka_servers,
                             group_id=str(tenant_id),
                             enable_auto_commit=False,
                             auto_offset_reset='latest')
    # popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess.Popen(args, stdout=open('stdout', 'a'), stderr=open('stderr', 'a'))

    msgs = consumer.poll(timeout_ms=time_out, max_records=1).values()
    print(msgs)
    if len(msgs) > 0 and len(list(msgs)[0]) > 0:
        msg = list(msgs)[0][0]
    else:
        raise Exception('Time Out')
    print(msg)
    print(msg.value.decode('utf-8').replace('"', '\''))
    resp = jsonify(msg.value.decode('utf-8').replace('"', '\''))
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

def get_error_resp(e):
    resp = jsonify(str({'status': e.args[0]}))
    # 跨域设置
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/api/analysis/data', methods=['GET', 'POST'])
def data_analysis():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        args = ['spark-submit', '--class', 'edu.bupt.iot.spark.common.DataAnalysis', '/home/spark/iot.jar']
        args.append(str(data.get('tenantId', '-1')))
        args.append(str(data.get('deviceType', '-1')))
        args.append(str(data.get('deviceId', '-1')))
        args.append(str(data.get('startTime', '-1')))
        args.append(str(data.get('endTime', '-1')))
        args.append(str(data.get('partNum', '1')))
        print(args)
        resp = get_resp(args, time_out= 90 * 1000, tenant_id=str(data.get('tenantId', '-1')), topic_type='data')

        return resp

    except Exception as e:
        print(e.args)
        return get_error_resp(e)

@app.route('/api/analysis/device', methods=['GET', 'POST'])
def device_analysis():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        args = ['spark-submit', '--class', 'edu.bupt.iot.spark.common.DeviceAnalysis', '/home/spark/iot.jar']
        args.append(str(data.get('tenantId', '-1')))
        args.append(str(data.get('startTime', '-1')))
        args.append(str(data.get('endTime', '-1')))

        resp = get_resp(args, time_out=90 * 1000, tenant_id=str(data.get('tenantId', '-1')), topic_type='device')
        return  resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@app.route('/api/model/get-model', methods=['GET', 'POST'])
def get_model():
    sql = 'select * from data_model where 1 = 1'
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        if 'modelId' in data:
            sql = sql + ' and model_id = %d' % int(data.get('modelId'))
        db = mysql(**mysql_args)
        res = {'data':[]}
        for i, item in enumerate(db.select(sql)):
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

@app.route('/api/app/get-app', methods=['GET', 'POST'])
def get_app():
    sql = 'select * from app where 1 = 1'
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        if 'tenantId' in data:
            sql = sql + ' and tenant_id = %d' % int(data.get('tenantId'))
        if  'modelId' in data:
            sql = sql + ' and model_id = %d' % int(data.get('modelId'))
        if  'appId' in data:
            sql = sql + ' and app_id = %d' % int(data.get('appId'))
        db = mysql(**mysql_args)
        res = {'data': []}
        for i, item in enumerate(db.select(sql)):
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
            res['data'].append(tmp)
        print(res)
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@app.route('/api/app/create-app', methods=['GET', 'POST'])
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
        sql_insert = "insert into app(app_id, app_name, model_id, app_input, app_output, tenant_id)" \
                     + " values(%d, '%s', %d, '%s', '%s', %d)" \
                      % (app_id, app_name, model_id, app_input, app_output, tenant_id)
        #print(sql_insert)
        db.insert(sql_insert)
        db.close()
        resp = jsonify(str({'status': 'create app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@app.route('/api/app/start-app', methods=['GET', 'POST'])
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
        timeout = int(data.get('timeout', 3600))
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
        db.close()
        resp = jsonify(str({'status': 'start app success!'}))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@app.route('/api/app/real-predict', methods=['GET', 'POST'])
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


@app.route('/api/app/stop-app', methods=['GET', 'POST'])
def stop_app():
    try:
        # data = {}
        # if request.method == 'GET':
        #     data = request.args
        # elif request.method == 'POST':
        #     data = request.form
        # print(data)
        # assert 'appId' in data, 'missing parameters app id!'
        # app_id = int(data.get('appId'))
        status =  request.environ['HTTP_PRE_STATUS']
        resp = jsonify({'status': status})
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)
"""
