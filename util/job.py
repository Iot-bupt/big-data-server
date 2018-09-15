import threading
from sklearn.externals import joblib
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import sys
sys.path.append("..")
from db.mysql import mysql
from config import mysql_args as db_args

class job(threading.Thread):

    def __init__(self, app_id, model_path, app_input, kafka_servers, timeout=60, mysql_args=db_args):
        super(job, self).__init__()
        self.__flag = threading.Event()     # 用于暂停线程的标识
        self.__flag.set()       # 设置为True
        self.__running = threading.Event()      # 用于停止线程的标识
        self.__running.set()      # 将running设置为True
        self.app_id = app_id
        self.app_input = {}
        self.timeout = timeout
        #self.kafka_servers = kafka_servers
        self.model = joblib.load(model_path)
        self.consumer = KafkaConsumer('deviceData',
                                      bootstrap_servers=kafka_servers,
                                      group_id='app_'+str(self.app_id))
                                      # enable_auto_commit=False,
                                      # auto_offset_reset='latest')
        self.producer = KafkaProducer(bootstrap_servers = kafka_servers)
        self.topic = str(app_id)
        for i, item in enumerate(app_input):
            v = self.app_input.setdefault(item['device_id'], {})
            v.setdefault(item['type'], i)
        #print(self.app_input)
        self.mysql_args = mysql_args

    def run(self):
        data = [0.] * len(self.app_input)
        while self.__running.isSet():
            if int(time.time()) % self.timeout == 0:
                db = mysql(**self.mysql_args)
                sql_select = "select * from app where app_id = %d and stop > 0" % (self.app_id)
                if len(list(db.select(sql_select))) > 0:
                    print(self.app_id, " stoped!")
                    db.close()
                    break
                db.close()
            self.__flag.wait()  # 为True时立即返回, 为False时阻塞直到内部的标识位为True后返回
            try:
                msg_tmp = list(self.consumer.poll(timeout_ms=5000, max_records=1).values())[0][0]\
                    .value.decode('utf-8').replace('\'', '\"')
            except:
                continue
            #print(list(self.consumer.poll(timeout_ms=5000, max_records=1).values())[0])
            #print(msg_tmp)
            msg_value = json.loads(msg_tmp)
            device_id = msg_value['deviceId']
            if msg_value['deviceId'] in self.app_input:
                for item in msg_value['data']:
                    device_type = item['key']
                    device_type2id = self.app_input[device_id]
                    if device_type in device_type2id:
                        data[device_type2id[device_type]] = item['value']
                        ret = self.model.predict([data])[0]
                        self.producer.send(self.topic, str(ret).encode('utf-8'))
                        print(self.app_id, " is running, predict result is ", ret)

    def pause(self):
        self.__flag.clear()     # 设置为False, 让线程阻塞

    def resume(self):
        self.__flag.set()    # 设置为True, 让线程停止阻塞

    def stop(self):
        self.__flag.set()       # 将线程从暂停状态恢复, 如何已经暂停的话
        self.__running.clear()        # 设置为False

if __name__ == '__main__':
    #t = job(1,2,3,4)
    import pickle
    print(mysql)
    #pickle.dumps(t)