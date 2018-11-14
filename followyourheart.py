from config import *
import random, math, time

followyourheart = Blueprint('dashboard', __name__)

def get_date():
    start = 1559347200
    end = 1654041600
    t = random.randint(start, end)  # 在开始和结束时间戳中随机取出一个
    date1_touple = time.localtime(t)  # 将时间戳生成时间元组
    date1 = time.strftime("%Y-%m-%d", date1_touple)  # 将时间元组转成格式化字符串（1976-05-21）
    t = t - random.randint(30*24*3600, 3*30*24*3600)  # 将时间戳生成时间元组
    date2_touple = time.localtime(t)  # 将时间戳生成时间元组
    date2 = time.strftime("%Y-%m-%d", date2_touple)  # 将时间元组转成格式化字符串（1976-05-21）
    return [date1, date2]

def get_rate():
    start = random.randint(1000, 4000)
    end = random.randint(0, 500)
    t = list(range(-start, -end, (-end + start) // 7))
    res = [math.exp(i / 1000) for i in t]
    return res

@followyourheart.route('/maintenance', methods=['GET'])
def maintenance():
    try:
        res = [
            get_rate()+get_date()
            for _ in range(5)
        ]
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/failure-reason', methods=['GET'])
def get_failure_reason():
    try:
        res= [random.randint(100, 200) for _ in range(3)] +\
        [random.uniform(0, 5)] + [random.randint(0,  100)  for _ in range(3)]
        res = [res, res,  res]
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/failure-rate', methods=['GET'])
def get_failure_rate():
    try:
        res= [random.random() for _ in range(365)]
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/user-satis-reason', methods=['GET'])
def get_user_satas_reason():
    try:
        res= {

             }
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/user-satis-level', methods=['GET'])
def get_user_satas_level():
    try:
        res= {
                "差": [random.randint(300, 3000) for _ in range(7)],
                "较差": [random.randint(300, 3000) for _ in range(7)],
                "一般": [random.randint(300, 3000) for _ in range(7)],
                "较好": [random.randint(300, 3000) for _ in range(7)],
                "好": [random.randint(300, 3000) for _ in range(7)],
                "bug": [random.randint(300, 3000) for _ in range(7)],
                "请求太慢": [random.randint(300, 3000) for _ in range(7)],
                "安全性不够": [random.randint(300, 3000) for _ in range(7)],
                "不符合需求": [random.randint(300, 3000) for _ in range(7)],
                "操作繁琐": [random.randint(300, 3000) for _ in range(7)],
                "专业性太强": [random.randint(300, 3000) for _ in range(7)],
                "服务器不稳定": [random.randint(300, 3000) for _ in range(7)],
                "其他": [random.randint(300, 3000) for _ in range(7)],
             }
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/device-count', methods=['GET'])
def get_device_count():
    try:
        res= {
                "压力传感器": [random.randint(300, 3000) for _ in range(7)],
                "速率传感器": [random.randint(300, 3000) for _ in range(7)],
                "形变传感器": [random.randint(300, 3000) for _ in range(7)],
                "光照传感器": [random.randint(300, 3000) for _ in range(7)],
                "湿度传感器": [random.randint(300, 3000) for _ in range(7)],
                "温度传感器": [random.randint(300, 3000) for _ in range(7)],
             }
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)


@followyourheart.route('/device-type', methods=['GET'])
def get_device_type():
    try:
        res= {
                 "压力传感器": random.randint(200, 2000),
                 "湿度传感器": random.randint(200, 2000),
                 "速率传感器": random.randint(200, 2000),
                 "形变传感器": random.randint(200, 2000),
                 "光照传感器": random.randint(200, 2000),
                 "温度传感器": random.randint(200, 2000)
             }
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)

@followyourheart.route('/device-status', methods=['GET'])
def get_device_status():
    try:
        res = {
                "暂停": [random.randint(100, 1000) for _ in range(7)],
                "运行": [random.randint(100, 1000) for _ in range(7)],
                "离线": [random.randint(100, 1000) for _ in range(7)]
                }
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)
