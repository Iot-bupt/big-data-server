import time
print(time.time())
print(time.localtime())
# print(time.strftime("%Y-%m-%d %H:%M:%S", time.time()))
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()+3600)) )
"""
import json
a = '[1, 2, 3]'
print(json.loads(a))

a = [1, 2, 3]
print(json.dumps(a))



print(json.dumps({'1':1, 'a':"aaa"}))
a = ["tem", "hum"]
b = ["id1", "id2"]
c = []
for device_id, data_type in zip(b, a):
    c.append({"device_id": device_id, "type": data_type})
print(json.dumps(c))
"""
"""
from kafka import KafkaConsumer, KafkaProducer
consumer = KafkaConsumer('2', bootstrap_servers = ['172.30.26.6:9092'], group_id = '-2', )
for msg in consumer:
    print(msg)
    #print (msg.value.decode('ascii'))
    print(msg.value.decode('utf-8'))
"""