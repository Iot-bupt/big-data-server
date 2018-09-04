from sqlalchemy import create_engine

def get_mysql_engine(mysql_args):
    engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s'%
                       (mysql_args['user'], mysql_args['passwd'], mysql_args['host'],  mysql_args['port'], mysql_args['dbname']),
                           connect_args={'charset': 'utf8'})
    return engine

def get_other_engine(mysql_args):
    engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s'%
                       (mysql_args['user'], mysql_args['passwd'], mysql_args['host'],  mysql_args['port'], mysql_args['dbname']),
                           connect_args={'charset': 'utf8'})
    return engine