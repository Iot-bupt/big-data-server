import os
import pymysql
from sqlalchemy import create_engine

#将数据库表的结构提取出放入新表，新表包含四个字段，即表名，字段名，字段类型，是否是主键
class DBTool():

    conn = None
    cursor = None
    #连接数据库
    def __init__(self, conn_dict={'host': '39.104.165.155', 'port': 3306, 'user': 'root', 'password': 'root', 'dbname': 'BUPT_IOT'}):
        self.conn = pymysql.connect(conn_dict['host'],
                                    conn_dict['user'],
                                    conn_dict['password'],
                                    conn_dict['dbname'])
        self.cursor = self.conn.cursor()

    #SQL语句执行方法
    def execute_query(self, sql_string):
        try:
            cursor=self.cursor
            cursor.execute(sql_string)
            self.conn.commit()
            return list
        except pymysql.Error as e:
            print("mysql execute error:", e)
            raise

    def execute_noquery(self, sql_string):
        try:
            cursor = self.cursor
            cursor.execute(sql_string)
            self.conn.commit()
        except pymysql.Error as e:
            print("mysql execute error:", e)
            raise
    #将数据库表的结构提取
    def select(self, sql):
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
            while row:
                yield row
                row = self.cursor.fetchone()
        except Exception as e:
            print(e)
            print("Error: unable to fetch data")

    def get_tables(self):
        for table_name in list(self.select("show tables")):
            table_name = table_name[0]
            table = {'table_name': table_name}
            table_info = list(self.select("desc %s" % table_name))
            table['field_name'] = [item[0] for item in table_info]
            table['field_type'] = [item[1] for item in table_info]
            table['key_type'] = [item[3] for item in table_info]
            yield table

    #将提取的表结构插入新表
    def inesrt_table(self):
        #建立新表
        # sql_newtable = "create table new_table(table_na VARCHAR(50),field_name VARCHAR(50),field_type VARCHAR(50),key_type INT DEFAULT 1 )"
        # rs = self.execute_query(sql_newtable)
        # if rs:
        #     print('setup success')
        # else:
        #     print('setup fail')
        #实现插入新表
        tables = list(self.get_tables())
        print(tables)
        for table in tables:
            table_name = table['table_name']
            field = table['field_name']
            field_type = table['field_type']
            key_type = table['key_type']
            field_len = table['field_name'].__len__()
            # print(field_len)
            for row in range(field_len):
                # print(row)
                # sql_insertkey = "INSERT INTO new_table(table_na,field_name,field_type,key_type) VALUES ('list(tables['table_name'])','list(tables['field_name[row]'])','list(tables['field_type[row]'])',1)"
                if key_type[row] == 'PRI':
                    key = 1
                else:
                    key =0
                sql_insertkey = "INSERT INTO new_table(table_na,field_name,field_type,key_type) VALUES ('%s','%s','%s',%d)" % (table_name,field[row],field_type[row],key)
                self.execute_query(sql_insertkey)
        print("success")
        self.close()

    def close(self):
        self.conn.close()

    # 封装方法，通过表名实现增删改，通过字段名实现增删改以及通过所有四个字段实现查询
    '''
    pri 1 为主键 0 为非主键
    type 字段类型
    column 字段名 
    sheet 表名
    '''
    def addColumn(self,sheet,column,type,pri):
        try:
            sql= ""
            if pri == 0:
                sql = "ALTER TABLE "+sheet+"ADD "+column+" "+type+";"
                self.cursor.execute(sql)
            else:
                prisql = "SELECT COUNT(*) PrimaryNum FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE t WHERE t.TABLE_NAME = '" + sheet + "'"
                self.execute_query(prisql);
                count = self.cursor.fetchall()[0]
                if count ==0:
                    sql = "ALTER TABLE "+sheet+" ADD "+column+" "+type+" PRIMARY KEY;"
                    self.cursor.execute(sql)
                else:
                    sql = "ALTER TABLE "+sheet+" DROP PRIMARY KEY "
                    self.cursor.execute(sql)
                    sql = "ALTER TABLE "+sheet+" ADD "+column+" "+type+" PRIMARY KEY;"
                    self.cursor.execute(sql)
            # sql = sql +"INSERT INTO new_table (table_na,field_name,field_type,key_type) VALUE ("+sheet+","+column+","+type+","+pri+");"
        except pymysql.Error as e:
            print(e)
            raise
    '''
    删除表字段
    '''
    def deleteColumn(self,sheet,column):
        try:
            # sql = "ALTER TABLE "+sheet+ " DROP COLUMN "+column+" ; DELETE FROM new_table WHERE table_na= "+sheet+" and field_name = "+column;
            sql = "ALTER TABLE " + sheet + " DROP COLUMN " + column ;
            self.cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            raise
    '''
    修改表字段名称
    '''
    def alterColumnName(self,sheet,column,name):
        try:
            typesql = "SELECT COLUMN_TYPE,COLUMN_DEFAULT,COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS  WHERE  TABLE_NAME = '"+sheet+"' and COLUMN_NAME = '" +column+"'"
            self.execute_query(typesql)
            info = self.cursor.fetchall()[0]
            if info[1] == None :
                sql = "alter  table "+sheet+" change "+column+ " "+name+" "+info[0]+" DEFAULT NULL COMMENT '"+info[2]+"'"
            else:
                sql = "alter  table "+sheet+" change "+column+ " "+name+" "+info[0]+" DEFAULT "+info[1]+" COMMENT '"+info[2]+"'"
            self.cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            raise

    '''
    修改表的字段属性
    '''
    def alterColumnType(self,sheet,column,type):
        try:
            # sql = "ALTER TABLE"+sheet +" MODIFY COLUMN" +column+" "+type+ ";UPDATE new_table SET field_type= "+type+" where table_na="+sheet+"and field_name ="+column+";"
            sql = "ALTER TABLE " + sheet + " MODIFY COLUMN " + column + " " + type ;
            self.cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            raise

    '''
    设置表的主键
    '''
    def addColumnPri(self,sheet,column):
        try:
            sql = ""
            prisql = "SELECT COUNT(*) PrimaryNum FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE t WHERE t.TABLE_NAME = '" + sheet + "'"
            self.execute_query(prisql);
            count = self.cursor.fetchall()[0]
            if count[0] == 0:
                sql = "ALTER TABLE " + sheet + " ADD  PRIMARY KEY ("+ column + ")"
                self.cursor.execute(sql)
            else:
                sql = "ALTER TABLE " + sheet + "  DROP PRIMARY KEY"
                self.cursor.execute(sql)
                sql = "ALTER TABLE " + sheet + "ADD  PRIMARY KEY (" + column + ")"
                self.cursor.execute(sql)
            # sql = sql + ";UPDATE new_table SET key_type= 1 where table_na="+sheet+"and field_name ="+column+";"
        except pymysql.Error as e:
            print(e)
            raise

    '''
    删除表的主键
    '''
    def deleteColumnPri(self,sheet):
        try:
            sql = ""
            prisql = "SELECT COUNT(*) PrimaryNum FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE t WHERE t.TABLE_NAME = '" + sheet + "'"
            self.execute_query(prisql);
            count = self.cursor.fetchall()[0]
            if count[0] == 1:
                sql = "ALTER TABLE" + sheet + " DROP PRIMARY KEY;"
                self.cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            raise


    '''
    根据表名增加表
    '''
    def insertTables(self,table_name):
        try:
            data={}
            sql = "INSERT INTO new_table(table_na)VALUES('%s') "%(table_name)
            result = self.execute_query(sql)
            if result:
                print('insert success')
            else:
                print('insert fail')
        except pymysql.Error as e:
            print(e)
            raise

    '''
    根据表名删除表
    '''

    def deleteTables(self, table_name):
        try:
            data = {}
            sql = "DELETE FROM new_table WHERE table_na = '%s'" % (table_name)
            result = self.execute_query(sql)
            if result:
                print('delete success')
            else:
                print('delete fail')
        except pymysql.Error as e:
            print(e)
            raise

    '''
    根据表名更新表
    '''
    def updateTables(self, table_name):
        try:
            data = {}
            sql = "UPDATE new_table SET table_na = '%s' " % (table_name)
            result = self.execute_query(sql)
            if result:
                print('delete success')
            else:
                print('delete fail')
        except pymysql.Error as e:
            print(e)
            raise

    '''
    根据所有字段实现查询
    '''
    def search(self,table_name,field_name):
        try:
            data = {}
            sql1 = "SELECT table_na FROM new_table WHERE field_name = '%s'"%(field_name)
            rs1 = self.execute_query(sql1)
            if rs1 == table_name:
                sql2 = "SELECT * FROM new_table WHERE field_name = '%s'"%(field_name)
                rs2 = self.execute_query(sql2)
                print(rs2)
            else:
                sql3 = "SELECT * FROM new_table WHERE table_na = '%s'"%(table_name)
                sql4 = "SELECT * FROM new_table WHERE field_name = '%s'"%(field_name)
                rs3 = self.execute_query(sql3)
                rs4 = self.execute_query(sql4)
                print(rs3)
                print(rs4)
        except pymysql.Error as e:
            print(e)
            raise








if __name__ == '__main__':
    a = DBTool()
    a.inesrt_table()
    # print(list(a.get_tables()))