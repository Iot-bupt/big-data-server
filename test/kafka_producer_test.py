from kafka import KafkaConsumer, KafkaProducer
import json
p = KafkaProducer(bootstrap_servers = ['kafka-service:9092'])
# Assign a topic
topic = 'deviceData'
import time
import random
a = {"deviceId": "1","tenantId": 1,"data": [{"key": "tem","ts": 1524708830000,"value": 1.00}]}
b = {"deviceId": "2","tenantId": 1,"data": [{"key": "hum","ts": 1524708830000,"value": 1.00}]}
def test():
    while (True):
        a["data"][0]["value"] = random.random() * 2 - 1
        p.send(topic, json.dumps(a).encode())
        print(json.dumps(a))
        time.sleep(1)
        b["data"][0]["value"] = random.random() * 2 - 1
        p.send(topic, json.dumps(b).encode())
        print(json.dumps(b))
if __name__ == '__main__':
    test()