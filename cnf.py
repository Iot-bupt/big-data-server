import multiprocessing

bind = '0.0.0.0:8092'
workers = multiprocessing.cpu_count() * 2 + 1
#work_class = 'geventwebsocket.websocket.WebSocket'#.gunicorn.workers.GeventWebSocketWorker'

"""
import json
from util.job import *
from util.mysql import *
from iot_web import kafka_servers, mysql_args
app2job = {}
def pre_start_app(app_id, app_job):
    print('\n\n', app_job, '\n\n')
    
    print('\n\n', app_job, '\n\n')
    return app_job

def pre_stop_app(app_id, app_job):
    print('\n\n', app_job, '\n\n')
    assert app_id in app_job, 'the app does not exist!'
    app_job[app_id].stop()
    del app_job[app_id]
    print('\n\n', app_job, '\n\n')
    return app_job

def get_app_id(query):
    data = {}
    for item in query.split('&'):
        tmp = item.split('=')
        if len(tmp) == 2:
            data.setdefault(tmp[0], int(tmp[1]))
    assert 'appId' in data, 'missing parameters app id!'
    return data['appId']
def pre_request(worker, req):
    global app2job
    if req.path == '/api/app/start-app':
        try:
            app_id = get_app_id(req.query)
            app2job = pre_start_app(app_id, app2job)
            req.headers.append(('PRE_STATUS', 'start app success!'))
        except Exception as e:
            req.headers.append(('PRE_STATUS', e.args))

    elif req.path == '/api/app/stop-app':
        try:
            app_id = get_app_id(req.query)
            app2job = pre_stop_app(app_id, app2job)
            req.headers.append(('PRE_STATUS', 'stop app success!'))
        except Exception as e:
            req.headers.append(('PRE_STATUS', e.args))
pre_request = pre_request
"""
