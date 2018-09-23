from config import *

mining = Blueprint('mining', __name__)

def get_data(engine, source_table, feature_columns):
    data = pd.read_sql(source_table, engine)
    X_train = data[feature_columns]
    return  X_train

def fit(data_model, x):
    data_model.fit(x)

def define_model(n_clusters):
    return KMeans(n_clusters)

@mining.route('/k-means', methods=['GET', 'POST'])
def train_model():
    try:
        data = {}
        if request.method == 'GET':
            data = request.args
        elif request.method == 'POST':
            data = request.form
        print(data)

        db_con_args = mysql_args
        if 'host' in data and 'user' in data and 'passwd' in data and 'dbname' in data:
            db_con_args['host'] = data['host']
            db_con_args['user'] = data['user']
            db_con_args['passwd'] = data['passwd']
            db_con_args['dbname'] = data['dbname']
            db_con_args['port'] = int(data.get('port', 3306))
        engine = get_mysql_engine(db_con_args)

        assert 'sourceTable' in data, 'missing parameters source table!'
        source_table = data['sourceTable']

        assert 'featureColumns' in data, 'missing parameters feature columns!'
        feature_columns = json.loads(data['featureColumns'])
        for item in feature_columns:
            assert isinstance(item, str), 'feature columns parameter format wrong!'

        assert 'n_clusters' in data, 'missing parameters n_clusters!'
        n_clusters = int(data['n_clusters'])

        data_model = define_model(n_clusters)
        x = get_data(engine, source_table, feature_columns)
        data_model.fit(x)

        res = {}
        res['labels'] = data_model.labels_.tolist()
        res['cluster_centers'] = data_model.cluster_centers_.tolist()
        resp = jsonify(res)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
    except Exception as e:
        print(e)
        return get_error_resp(e)
