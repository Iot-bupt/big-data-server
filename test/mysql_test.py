"""
import pymysql

db = pymysql.connect('39.104.165.155', 'root', 'root', 'BUPT_IOT')

cursor = db.cursor()

sql = 'show tables'
cursor.execute(sql)
data = cursor.fetchall()
print(data)

sql = 'desc data_model'
cursor.execute(sql)
data = cursor.fetchall()
print(data)
"""

import util.mysql as mysql
db = mysql.mysql()
# da = list(db.select('show tables'))
# print(da)
# da = list(db.select('desc recent'))
# print(da)
# da = list(db.select('select * from data_model'))
# print(da)
import random
a = ['humidity', 'temperature', 'pressure', 'light', 'velocity', 'deformation']
b = ['1d', '3d', '1w', '1m']
for j in range(50):
    for k in range(6):
        for t in range(4):
            db.insert("insert into recent_device values(2, '%s', %d, %d, %d, %f, '%s', '%s')" \
                        % (a[k],random.randint(100, 100000), random.randint(100, 10000000), random.randint(100, 10000000), random.random(),b[t], '2018-06-'+str(random.randint(1, 30))))
