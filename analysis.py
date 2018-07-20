from config import *

analysis = Blueprint('analysis', __name__)

def get_resp(args, time_out = 90 * 1000, tenant_id = '-1', topic_type = 'data'):
    consumer = KafkaConsumer(str(tenant_id + '_' + topic_type),
                             bootstrap_servers=kafka_servers,
                             group_id=str(tenant_id),
                             enable_auto_commit=False,
                             auto_offset_reset='latest')
    # popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess.Popen(args, stdout=open('stdout', 'a'), stderr=open('stderr', 'a'))
    """
    while True:
        line = popen.stdout.readline().strip().decode('utf-8') # 获取内容
        if line:
            print (line)
        else:
            break
    """
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

@analysis.route('/data', methods=['GET', 'POST'])
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

@analysis.route('/device', methods=['GET', 'POST'])
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

@analysis.route('recent-device', methods=['GET', 'POST'])
def recent_device_analysis():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        assert 'flag' in data, 'missing parameters flag'
        flag = data['flag']
        db = mysql(**mysql_args)
        sql_select = "select device_type, device_count, data_count, usual_data_count, usual_data_rate" \
                     + " from recent_device where tenant_id = %d and flag = '%s'" % (tenant_id, flag) \
                     + " and date in" \
                     + " (select max(date) from recent_device where tenant_id = %d and flag = '%s')" \
                       % (tenant_id, flag)
        res = {}
        for item in db.select(sql_select):
            device_count = res.setdefault('deviceCount', {}); device_count[item[0]] = item[1]
            data_count = res.setdefault('dataCount', {}); data_count[item[0]] = item[2]
            usual_data_count = res.setdefault('usualDataCount', {}); usual_data_count[item[0]] = item[2]
            usual_data_rate = res.setdefault('usualDataRate', {}); usual_data_rate[item[0]] = float(item[4])
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@analysis.route('recent-data', methods=['GET', 'POST'])
def recent_data_analysis():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)
        assert 'tenantId' in data, 'missing parameters tenant id!'
        tenant_id = int(data['tenantId'])
        assert 'days' in data, 'missing parameters days!'
        days = int(data['days'])
        db = mysql(**mysql_args)
        sql_select = "select device_type, max_value, min_value, mean_value, stddev_value, data_count, date" \
                     + " from recent_data where tenant_id = %d" % (tenant_id) \
                     + " and date in" \
                     + " (select * from (select distinct(date) from recent_data where tenant_id = %d " % (tenant_id) \
                     + " order by date desc limit %d) as tmp)" % (days)
        print(sql_select)
        res = {}
        for item in db.select(sql_select):
            max_value = res.setdefault('maxValue', {}); time_max_value = max_value.setdefault(str(item[6]), {}); time_max_value[item[0]] = float(item[1])
            min_value = res.setdefault('minValue', {}); time_min_value = min_value.setdefault(str(item[6]), {}); time_min_value[item[0]] = float(item[2])
            mean_value = res.setdefault('meanValue', {});  time_mean_value = mean_value.setdefault(str(item[6]), {}); time_mean_value[item[0]] = float(item[3])
            stddev_value = res.setdefault('stddevValue', {}); time_stddev_value = stddev_value.setdefault(str(item[6]), {}); time_stddev_value[item[0]] = float(item[4])
            data_count = res.setdefault('dataCount', {}); time_data_count = data_count.setdefault(str(item[6]), {}); time_data_count[item[0]] = item[5]
        print(res)
        db.close()
        resp = jsonify(str(res))
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)