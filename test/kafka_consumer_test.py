from kafka import KafkaConsumer, KafkaProducer
import json
# connect to Kafka server and pass the topic we want to consume 10.112.233.200


# msg = consumer.poll(timeout_ms=600 * 1000, max_records=1)
# msg.values()
# print(type(msg))
# print(list(msg.values()))
consumer = KafkaConsumer('deviceData',
                        bootstrap_servers=['kafka-service:9092'],
                        group_id='-2')
for msg in consumer:
    print(msg)
    #print (msg.value.decode('ascii'))
    print(msg.value.decode('utf-8'))
    #break
