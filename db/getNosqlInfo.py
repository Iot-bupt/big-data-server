from cassandra.cluster import Cluster

class NoSQLTool():

    def __init__(self):
        self.cluster = Cluster(contact_points=['39.104.165.155'])
        self.session = self.cluster.connect('bupt_iot')

    def get_columns(self):
        try:
            sql = "select table_name from system_schema.tables where keyspace_name = 'bupt_iot'"
            keywords = self.session.execute(sql)
            #print(keywords)
            for row in keywords:
                print(row)
                query = "select * from system_schema.columns where keyspace_name = 'bupt_iot' and  table_name = '%s'"%(row)
                rs = self.session.execute(query)
                #print(rs)
                for rep in rs:
                    print(rep)
                yield rep

        except Exception as e:
            print(e)




if __name__ == '__main__':
    NoSQLTool().get_columns()

