from hdfs import *
client = Client('http://39.104.186.210:50070')
file_path = '/data/device-data-1527513600000/part-00001'
with client.read(file_path) as fs:
    content = fs.read()
    print(content)
