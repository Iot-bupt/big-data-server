from util.cmd_util import exec_cmd
from flask import Flask, request, render_template, abort
from geventwebsocket.handler import WebSocketHandler
from gevent.pywsgi import WSGIServer
import json
from kafka import KafkaConsumer
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/statistics', methods=['GET', 'POST'])
def statistics():
    if request.environ.get('wsgi.websocket'):
        ws = request.environ['wsgi.websocket']
        if ws is None:
            abort(404)
        else:
            try:
                message = ws.receive()
                print(message)
                jar_args = json.loads(message)
                args = ['spark-submit', '--class', 'edu.bupt.iot.spark.common.Statistics', '/home/spark/iot.jar']
                args.append(str(jar_args.get('tenantId', '-1')))
                args.append(str(jar_args.get('deviceType', '-1')))
                args.append(str(jar_args.get('deviceId', '-1')))
                args.append(str(jar_args.get('startTime', '-1')))
                args.append(str(jar_args.get('endTime', '-1')))
                consumer = KafkaConsumer(str(jar_args.get('tenantId', '-1')),
                                         bootstrap_servers=['10.108.218.64:9092'],
                                         group_id=str(jar_args.get('tenantId', '-1')),
                                         enable_auto_commit=False,
                                         auto_offset_reset='latest')
                print(args)
                popen = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                #print(args):
                for msg in consumer:
                    ws.send(msg.value.decode('utf-8'))
                    #ws.send(str(msg))
                    break
            except:
                print('error')
    return ''

@app.route('/websocket', methods=['GET', 'POST'])
def echo():
    print(request.environ.get('wsgi.websocket'))
    if request.environ.get('wsgi.websocket'):
        ws = request.environ['wsgi.websocket']
        if ws is None:
            abort(404)
        else:
            while True:
                if not ws.closed:
                    message = ws.receive()
                    print(message)
                    ws.send(message)
                else:
                    break
if __name__ == '__main__':
    # app.run(port=8989, host='0.0.0.0',debug=True)
    app.debug=True
    http_server = WSGIServer(('0.0.0.0', 8989), app, handler_class=WebSocketHandler)
    http_server.serve_forever()
