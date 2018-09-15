import pymysql
import pandas as pd
from sqlalchemy import create_engine

class mysql():
    def __init__(self, host='39.104.165.155', user='root', passwd='root', dbname='BUPT_IOT', port=3306):
        self.db = pymysql.connect(host, user, passwd, dbname, port)
        self.cursor = self.db.cursor()
        self.engine = create_engine('mysql+pymysql://'+user+':'+passwd+'@'+host+':3306/'+dbname)

    def insert(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def delete(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

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
            #print(table_name)
            table = {'table_name': table_name}
            table['cloumns'] = [item[0] \
                                for item in list(self.select("desc %s" % table_name))]
            #print(table)
            yield table

    def close(self):
        self.db.close()

    def sql_exec(self,sql):
        try:
            df = pd.read_sql_query(sql, self.engine)
            return df
        except Exception as e:
            print(e)
            print("Error: unable to execute sql query")


if __name__ == '__main__':
    db = mysql()
    print(list(db.get_tables()))

