import os
import pymysql
from sqlalchemy import create_engine


class DBTool:

    conn = None
    cursor = None

    def __init__(self, conn_dict):
        self.conn = pymysql.connect(host=conn_dict['host'],
                                    user=conn_dict['user'],
                                    passwd=conn_dict['password'],
                                    dbname=conn_dict['dbname'])
        self.cursor = self.conn.cursor()


    def execute_query(self, sql_string):
        try:
            cursor=self.cursor
            cursor.execute(sql_string)
            list = cursor.fetchall()
            cursor.close()
            self.conn.close()
            return list
        except pymysql.Error as e:
            print("mysql execute error:", e)
            raise

    def execute_noquery(self, sql_string):
        try:
            cursor = self.cursor
            cursor.execute(sql_string)
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
        except pymysql.Error as e:
            print("mysql execute error:", e)
            raise

    def main(self):
        conn_dict = {'host': '39.104.165.155',  'user': 'root', 'password': 'root', 'dbname': 'BUPT_IOT'}
        conn = DBTool(conn_dict)
        sql_gettables = "select table_name from  information_schema.`TABLES` WHERE TABLE_SCHEMA = 'BUPT_IOT';"
        list = conn.execute_query(sql_gettables)

        sql_newtable = "create table new_table(table_na VARCHAR(20),field_name VARCHAR(20),field_type VARCHAR(20),key_type INT DEFAULT 1 )"
        new_table = conn.execute_query(sql_newtable)

        if list:
            for row in list:
                print(row[0])
                sql_insertkey = "alter table new_table add (table_na,field_name,field_type,key_type) "
                result = conn.execute_query(sql_insertkey)
                if result:
                    print(result)
                else:
                    print('export fail')



if __name__ == '__main__':
    DBTool()