from config import *


list = Blueprint('list', __name__)


@list.route('/insertTables',methods=['POST'])
def insertTables():
    try:
        data = {}
        sql = "INSERT INTO new_table(table_na)VALUES(?) "
        db=mysql()
        result = db.sql_exec(sql)
        db.close()
        if result:
            print('insert success')
        else:
            print('insert fail')
    except Exception as e:
        print(e)
        return get_error_resp(e)


@list.route('/deleteTables',methods=['DELETE'])
def deleteTables():
    try:
        data={}
        sql = "DELETE FROM new_table WHERE table_na = '?'"
        db=mysql()
        result = db.sql_exec(sql)
        db.close()
        if result:
            print('delete success')
        else:
            print('delete fail')
    except Exception as e:
        print(e)
        return get_error_resp(e)

@list.route('/updateTables',methods=['POST'])
def updateTables():
    try:
        data={}
        sql = "UPDATE new_table SET table_na = '?' "
        db=mysql()
        result = db.sql_exec(sql)
        db.close()
        if result:
            print('update success')
        else:
            print('update fail')
    except Exception as e:
        print(e)
        return get_error_resp(e)

