import os
import json
import time
import subprocess
from datetime import date
from kafka import KafkaConsumer
from flask import Blueprint, jsonify, request,Flask
from flask_socketio import SocketIO
from db.mysql import mysql
from util.job import job
from util.error import get_error_resp
model_path = '/home/model/'
kafka_servers = ['kafka-service:9092']
mysql_args = {'host':'172.24.32.169', 'user':'root', 'passwd':'root', 'dbname':'BUPT_IOT'}