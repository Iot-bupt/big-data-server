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
            list = cursor.fetchall()
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

    def main(self):
        # conn_dict = {'host': '39.104.165.155', 'port': 3306, 'user': 'root', 'password': 'root', 'dbname': 'BUPT_IOT'}
        # conn = DBTool(self.conn_dict)
        sql_gettables = "select table_name from  information_schema.`TABLES` WHERE TABLE_SCHEMA = 'BUPT_IOT';"
        list = self.execute_query(sql_gettables)

        # sql_newtable = "create table new_table(table_na VARCHAR(20),field_name VARCHAR(20),field_type VARCHAR(20),key_type INT DEFAULT 1 )"
        # rs = self.execute_query(sql_newtable)
        # if rs:
        #     print('setup success')
        # else:
        #     print('setup fail')

        if list:
            for row in list:
                # print(row[0])
                sql_insertkey = "INSERT INTO new_table(table_na,field_name,field_type,key_type) VALUES ('b','a','a',1)"
                result = self.execute_query(sql_insertkey)
                print(result)
                # if result:
                #     print(result)
                # else:
                #     print('export fail')



if __name__ == '__main__':
    a = DBTool()
    a.main()