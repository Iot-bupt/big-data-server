from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def get_cassandra_session(args):
    auth_provider = None
    if 'user' in args and 'passwd' in args:
        CASSANDRA_USER = None
        CASSANDRA_PASS = None
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)

    CASSANDRA_HOST = args['host']
    CASSANDRA_PORT = args.get('port', 9042)
    CASSANDRA_DB = args['db']
    cluster = Cluster(contact_points=CASSANDRA_HOST, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect(CASSANDRA_DB)
    return session