import os
import pymysql
from sqlalchemy import create_engine


class DBTool():

    conn = None
    cursor = None

    def __init__(self, conn_dict={'host': '39.104.165.155', 'port': 3306, 'user': 'root', 'password': 'root', 'dbname': 'BUPT_IOT'}):
        self.conn = pymysql.connect(conn_dict['host'],
                                    conn_dict['user'],
                                    conn_dict['password'],
                                    conn_dict['dbname'])
        self.cursor = self.conn.cursor()


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
            # print(table_name)
            table = {'table_name': table_name}
            table_info = list(self.select("desc %s" % table_name))
            # print(list(table_info))
            table['field_name'] = [item[0] for item in table_info]
            field_len = list(table['field_name']).__len__()
            #print(field_len)
            table['field_type'] = [item[1] for item in table_info]
            table['key_type'] = [item[3] for item in table_info]
            yield table


    def inesrt_table(self):
        # sql_newtable = "create table new_table(table_na VARCHAR(20),field_name VARCHAR(20),field_type VARCHAR(20),key_type INT DEFAULT 1 )"
        # rs = self.execute_query(sql_newtable)
        # if rs:
        #     print('setup success')
        # else:
        #     print('setup fail')
        tables = self.get_tables()
        print(tables)
        #print(type(tables['field_name']))
        field_len = tables['field_name'].__len__()
        for row0 in tables:
            for row1 in range(field_len):
                sql_insertkey = "INSERT INTO new_table(table_na,field_name,field_type,key_type) VALUES ('list(tables['table_name'])','list(tables['field_name'])','list(tables['field_type'])','list(tables['key_type'])')"
                result = self.execute_query(sql_insertkey)
                #print(result)



if __name__ == '__main__':
    a = DBTool()
    #a.inesrt_table()
    a.get_tables()