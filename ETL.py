from config import *

etl = Blueprint('etl', __name__)

@etl.route('/table-struct', methods=['GET'])
def get_table_struct():
    try:
        data = request.args
        print(data)
        assert data is not None ,'missing args!'
        db_type = data.get('db_type', 'mysql')
        if db_type == 'nosql':
            db_con_args = cassandra_args
            if 'user' in data and 'passwd' in data:
                db_con_args['user'] = data['user']
                db_con_args['passwd'] = data['passwd']
            if 'host' in data:
                db_con_args['host'] = data['host'].split(',')
            if 'dbname' in data:
                db_con_args['dbname']= data['dbname']
            session = get_cassandra_session(db_con_args)
            res = list(get_cassandra_tables(session))
        else :
            if db_type == 'mysql':
                get_engine = get_mysql_engine
                db_con_args = mysql_args
                get_tables = get_mysql_tables
            else:
                get_engine = get_other_engine
                db_con_args = other_args
                get_tables = get_mysql_tables

            if 'host' in data and 'user' in data and 'passwd' in data and 'dbname' in data:
                db_con_args['host'] = data['host']
                db_con_args['user'] = data['user']
                db_con_args['passwd'] = data['passwd']
                db_con_args['dbname'] = data['dbname']
                db_con_args['port'] = int(data.get('port', 3306))
                engine = get_engine(db_con_args)
            else:
                engine = get_engine(db_con_args)
            res = list(get_tables(engine))
        print(res)
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e, trans2str=False)

@etl.route('/transform', methods=['POST'])
def transform():
    try:
        #data = json.loads(request.get_data().decode('utf-8'))
        data = request.get_json()
        assert data is not None, 'must post json object!'
        print(data)
        assert 'source_table' in data, 'missing parameters source table name!'
        source = data.get('source_table')
        target = source + '_transform'
        if  'target_table' in data:
            target = data['target']
        assert 'transform_args' in data, 'missing parameters transform arguments!'
        transform_args = data['transform_args']
        #print(transform_args)

        db_type = data.get('db_type', 'mysql')
        if db_type == 'nosql':
            db_con_args = data.get('db_con_args', cassandra_args)
            session = get_cassandra_session(db_con_args)
            db_data = Data(source, db_type='nosql',
                           source_session=session,
                           target_engine=get_mysql_engine(mysql_args))
        else:
            if db_type == 'mysql':
                get_engine = get_mysql_engine
                db_con_args = data.get('db_con_args', mysql_args)
                #print(db_con_args)
            else:
                get_engine = get_other_engine
                db_con_args = data.get('db_con_args', other_args)
            engine = get_engine(db_con_args)
            db_data = Data(source=source, source_engine=engine)
        #
        # db_con_args = {}
        # if 'host' in data and 'user' in data  and 'passwd' in data and 'dbname' in data:
        #     db_con_args['host'] = data['host']
        #     db_con_args['user'] = data['user']
        #     db_con_args['passwd'] = data['passwd']
        #     db_con_args['dbname'] = data['dbname']
        #     engine = get_mysql_engine(db_con_args)
        # else:
        #     engine = get_mysql_engine(mysql_args)
        # db_data = Data(source=source,source_engine=engine)
        count = db_data.transform(target=target,transform_args=transform_args,save=True)
        res = {'length of data before transform': count[0],
               'length of data after transform': count[1]}
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e, trans2str=False)

@etl.route('/upload-csv-file', methods=['POST'])
def upload_csv_file():
    try:
        # data = request.values.get('target_table')
        # print(data)
        assert 'csv_file' in request.files, 'must post csv file with key "csv_file"!"'
        file = request.files['csv_file']
        filename = file.filename
        df = pd.read_csv(file)
        #print(df)
        target = 'file_' + filename.replace('.csv', '')
        # data = None
        # try:
        #     #data = json.loads(request.values.get('args'))
        #     #.get('args')#
        #     print(data)
        # except:
        #     print('not a json string')
        data = request.values
        if data is not None:
            db_type = data.get('db_type', 'mysql')
            if db_type == 'mysql':
                get_engine = get_mysql_engine
                db_con_args = mysql_args
            else:
                get_engine = get_other_engine
                db_con_args = other_args

            if 'target_table' in data:
                target = data['target_table']
            if 'host' in data and 'user' in data and 'passwd' in data and 'dbname' in data:
                db_con_args['host'] = data['host']
                db_con_args['user'] = data['user']
                db_con_args['passwd'] = data['passwd']
                db_con_args['dbname'] = data['dbname']
                db_con_args['port'] = int(data.get('port', 3306))
                engine = get_engine(db_con_args)
            else:
                engine = get_engine(db_con_args)
        else:
            engine = get_mysql_engine(mysql_args)
        #engine = get_mysql_engine(mysql_args)
        length = len(df.index)
        df.to_sql(name=target, con=engine,if_exists='append', index=False)
        res = {'status':True,
               'database table name': target,
               'length of data  insert into database': length}
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e, trans2str=False)

