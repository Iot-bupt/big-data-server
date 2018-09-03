from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import pandas as pd

CASSANDRA_USER = ''
CASSANDRA_PASS = ''
CASSANDRA_HOST = '39.104.165.155'
CASSANDRA_PORT = ''
CASSANDRA_DB = ''
CASSANDRA_TABLE = ''

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster(contact_points=[CASSANDRA_HOST])
    #auth_provider=auth_provider)

session = cluster.connect(CASSANDRA_DB)
session.row_factory = pandas_factory#dict_factory
session.default_fetch_size = None

sql_query = "SELECT * FROM {}.{};".format(CASSANDRA_DB, CASSANDRA_TABLE)

rslt = session.execute(sql_query, timeout=None)
df = rslt._current_rows
print(df)