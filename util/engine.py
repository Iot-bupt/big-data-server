from sqlalchemy import create_engine

def get_mysql_engine(mysql_args):
    engine = create_engine('mysql+pymysql://%s:%s@%s:3306/%s'%
                       (mysql_args['user'], mysql_args['passwd'], mysql_args['host'], mysql_args['dbname']),
                           connect_args={'charset': 'utf8'})
    return engine