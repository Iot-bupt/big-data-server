model_path = '/home/model/'
kafka_servers = ['kafka-service:9092']
mysql_args = {'host':'192.168.1.159', 'user':'root', 'passwd':'Huahan123','port':3306, 'dbname':'BUPT_IOT'}
other_args = {'host':'192.168.1.159', 'user':'root', 'passwd':'Huahan123','port':3306, 'dbname':'BUPT_IOT'}
cassandra_args = {'host':['192.168.1.159'], 'dbname':'bupt_iot'}

import os, csv, json, time
import pandas as pd
import subprocess, pymysql
from datetime import date
from kafka import KafkaConsumer
from flask import Blueprint, jsonify, request,Flask,Response
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib
from flask_socketio import SocketIO
from db.mysql import mysql
from hdfs.file import file
from etl.data import Data
from etl.data import get_mysql_tables, get_cassandra_tables
from util.job import job
from util.error import get_error_resp
from util.engine import *
from util.session import get_cassandra_session


# if __name__ == '__main__':
#     print('mysql+pymysql://%s:%s@%s:3306/%s'%
#                        (mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['dbname']))