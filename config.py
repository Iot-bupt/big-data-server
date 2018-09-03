import os
import csv
import json
import time
import pandas as pd
import subprocess
import pymysql
from datetime import date
from kafka import KafkaConsumer
from flask import Blueprint, jsonify, request,Flask,Response
from flask_socketio import SocketIO
from db.mysql import mysql
from hdfs.file import file
from etl.data import Data
#from etl.data import get_tables_pandas as get_tables
from util.job import job
from util.error import get_error_resp
from util.engine import get_mysql_engine

model_path = '/home/model/'
kafka_servers = ['kafka-service:9092']
mysql_args = {'host':'172.24.32.169', 'user':'root', 'passwd':'root', 'dbname':'BUPT_IOT'}


# if __name__ == '__main__':
#     print('mysql+pymysql://%s:%s@%s:3306/%s'%
#                        (mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['dbname']))