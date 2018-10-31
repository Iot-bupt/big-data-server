import pandas as pd

def get_mysql_tables(engine):
    tables = pd.read_sql_query('show tables', engine)
    for table_name in [tables.iloc[i, 0] \
                       for i in range(tables.shape[0])]:
        table = {'table_name': table_name}
        sql = 'desc `%s`' % table_name
        each_table = pd.read_sql_query(sql, engine)
        table['columns'] = [each_table.iloc[j, 0] \
                            for j in range(each_table.shape[0])]
        yield table

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def get_cassandra_tables(session):
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    query = "select table_name from system_schema.tables where keyspace_name='%s'" \
            % session.keyspace
    print(query)
    result = session.execute(query)
    tables = result._current_rows
    for table_name in [tables.iloc[i, 0] \
                       for i in range(tables.shape[0])]:
        table = {'table_name': table_name}
        sql = "select column_name from system_schema.columns where keyspace_name='%s' and table_name='%s'" \
              % (session.keyspace, table_name)
        result = session.execute(sql)
        each_table = result._current_rows
        table['columns'] = [each_table.iloc[j, 0] \
                            for j in range(each_table.shape[0])]
        yield table


# from cassandra.cluster import Cluster
# cluster = Cluster(contact_points=['39.104.165.155'])
# session = cluster.connect('bupt_iot')
# a = list(get_cassandra_tables(session))
# print(a)
class Data():

    def __init__(self,
                 source,
                 db_type = 'sql',
                 source_engine=None,
                 source_session=None,
                 target=None,
                 target_engine=None,
                 transform_args=None):

        self.transform_args = transform_args
        self.source_engine = source_engine
        if target_engine is None:
            self.target_engine = source_engine
        else:
            self.target_engine = target_engine
        self.source = source
        self.target = target
        assert source is not None, 'source must be given!'
        if db_type == 'sql':
            assert source_engine is not None, 'source engine must be given when database type is sql'
            self.df = pd.read_sql(self.source,
                                  self.source_engine)
        elif db_type == 'nosql':
            assert source_session is not None, 'source session must be given when database type is nosql'
            assert target_engine is not None, 'target engine must be given when database type is sql'
            source_session.row_factory = pandas_factory
            source_session.default_fetch_size = None
            query = 'select * from ' + source
            result = source_session.execute(query)
            self.df = result._current_rows
            columns = self.df.columns.values.tolist()
            import uuid
            for column in columns:
                if type(self.df[column][0]) is uuid.UUID:
                    self.df[column] = self.df[column].astype(str)

    def filter(self, filter_args):
        # if filter_args is None:
        #     filter_args = self.args.get('filter', [])
        for item in filter_args:
            func = self.get_filter(item['cmp'], item['value'])
            self.df = self.df[func(self.df[item['column']])]

    def drop(self, drop_args):
        # if drop_args is None:
        #     drop_args = self.args.get('drop', [])
        self.df.drop(drop_args,
                     axis=1,
                     inplace=True)

    def dropna(self, dropna_args):
        # if dropna_args is None:
        #     dropna_args = self.args.get('dropna', [])
        self.df.dropna(subset=dropna_args,
                       inplace=True)

    def fillna(self, fillna_args):
        values = {}
        mean = self.df.mean()
        median = self.df.median()
        mode = self.df.mode().iloc[0]
        # if fillna_args is None:
        #     fillna_args = self.args.get('fillna', [])
        for item in fillna_args:
            column = item['column']
            if item['value'] == 'mean':
                value = mean[column]
            elif item['value'] == 'median':
                value = median[column]
            elif item['value'] == 'mode':
                value = mode[column]
            else:
                value =  item['value']
            values[column] = value

        self.df.fillna(values,
                       inplace=True)

    def split(self, split_args):
        for item in split_args:
            tmp = self.df[item['source_column']].str.split(item['split_flag'], expand=True)
            rename = {i:item['source_column'] + '_split_' + str(i) for i in tmp.columns}
            tmp.rename(columns=rename, inplace=True)
            if item.get('drop_source_column', 0) == 1:
                self.drop([item['source_column']])
            self.df = self.df.join(tmp)

    def merge(self, merge_args):
        for item in merge_args:
            new_column_name = \
                'merge_' + '_'.join(item['source_columns'])
            new_column_name = item.get('new_column_name', new_column_name)
            func = \
                self.get_merge(item['source_columns'], item['split_flag'])
            self.df[new_column_name] = self.df.apply(func, axis=1)
            if item.get('drop_source_columns', 0)  == 1:
                self.drop(item['source_columns'])

    def math_func(self, math_func_args):
        for item in math_func_args:
            new_column_name = \
                item['function'] + '_' + '_'.join(item['source_columns'])
            new_column_name = item.get('new_column_name', new_column_name)
            func = \
                self.get_function(item['source_columns'], item['function'])
            self.df[new_column_name] = self.df.apply(func, axis=1)
            if item.get('drop_source_columns', 0) == 1:
                self.drop(item['source_columns'])

    def rename(self, rename_args_list):
        rename_args_dict = {item[0]:item[1] for item in rename_args_list}
        self.df.rename(columns=rename_args_dict, inplace=True)

    def transform(self, target=None, target_engine=None, transform_args=None, save=False):
        s_l = self.__len__()
        if transform_args is None:
            transform_args = self.transform_args
        for op in transform_args:
            op_type = op.get('type', '')
            op_args = op.get('args', [])
            if op_type == 'drop':
                self.drop(op_args)
            elif op_type == 'dropna':
                self.dropna(op_args)
            elif op_type == 'filter':
                self.filter(op_args)
            elif op_type == 'fillna':
                self.fillna(op_args)
            elif op_type == 'split':
                self.split(op_args)
            elif op_type == 'merge':
                self.merge(op_args)
            elif op_type == 'math':
                self.math_func(op_args)
            elif op_type == 'rename':
                if op_args == []:
                    op_args = {}
                self.rename(op_args)
        if target is None:
            target = self.target
        if target_engine is None:
            target_engine = self.target_engine
        if save and target is not None and target_engine is not None:
            self.save(target, target_engine)
        e_l = self.__len__()
        return (s_l, e_l)

    def save(self, target=None, target_engine=None):
        if target is None:
            target = self.target
        if target_engine is None:
            target_engine = self.target_engine
        if target is not None and target_engine is not None:
            self.df.to_sql(name=target,
                           con=target_engine,
                           if_exists='append',
                           index=False)

    def get_merge(self, columns, split_flag):
        def _merge(x):
            res = \
                split_flag.join([str(x[item]) for item in columns])
            return res
        return _merge

    def get_function(self, columns, func):
        def _function(x):
            l = len(columns)
            tmp = [x[item] for item in columns]
            if func == 'sum':
                return sum(tmp)
            if func == 'mean':
                return sum(tmp)/l
            if func == 'max':
                return max(tmp)
            if func == 'min':
                return min(tmp)
        return _function

    def get_filter(self, cmp, value):
        def _filter(x):
            if cmp == '>':
                return x > value
            if cmp == '<':
                return x < value
            if cmp == '==':
                return x == value
            if cmp == '<=':
                return x <= value
            if cmp == '>=':
                return x >= value
            if cmp == '!=':
                return x != value
            if cmp == 'like':
                return x.str.contains(value)
            return x == x
        return _filter

    def __len__(self):
        return len(self.df.index)

if __name__ == '__main__':

    from sqlalchemy import create_engine
    import uuid
    engine = create_engine('mysql+pymysql://root:root@172.24.32.169:3306/BUPT_IOT?charset=utf8')
    from cassandra.cluster import Cluster

    cluster = Cluster(contact_points=['39.104.165.155'])

    session = cluster.connect('bupt_iot')

    # session.row_factory = pandas_factory
    # session.default_fetch_size = None
    data = Data(source='group', db_type='nosql', source_session=session,target='test_liwan', target_engine=engine)
    #print(data.df)
    sql_query = "SELECT * FROM group"
    result = session.execute(sql_query)
    df = result._current_rows

    # columns = df.columns.values.tolist()
    # for column in columns:
    #     if type(df[column][0]) is uuid.UUID:
    #         df[column] = df[column].astype(str)

    data.df = df
    #print(df)
    # data = Data(source='mydf', source_engine=engine)
    # #data.drop(['index', 'id'])
    # #data.dropna(['id', 'num'])
    #
    data.df = pd.DataFrame({'hah':['109', '1-1'],
                            'test':['123-456-789', '236-456-455'],
                            '1':[908, 201],
                            '2':[None, 755],
                            '3':[574,7665]})
    data.rename([["test", "test1"],["hah","test2"]])
    print(data.df)
    #data.save('test')
    # # print(data.df)
    # #
    # # data.filter([{'column':'test', 'cmp' : 'like', 'value':'-4'},
    # #              {'column': 'test', 'cmp': '==', 'value': '123-456-789'}])
    # #data.filter([{'column': 'index', 'cmp': '>=', 'value': 3}])
    # #data.fillna([{'column':'index', 'value':1000}, {'column':'id', 'value':'mean'}])
    # # data.split([{"source_column":"hah","split_flag":"-","drop_source_column":0},
    # #             {"source_column": "test", "split_flag": "-", "drop_source_column": 1}])
    # # data.merge([{"source_columns":["hah", "test"],"split_flag":"-","drop_source_columns":0},
    # #             {"source_columns": ["hah", "merge_hah_test"], "split_flag": "-", "drop_source_columns": 1}])
    # # data.rename({'1':'362846', '3':'34564', '6':'888'})
    # # data.math_func([{'function': 'mean', 'source_columns':['1','2', '3'], 'new_column_name':'ttsdffds'},
    # #                 {'function': 'min', 'source_columns': ['1', '2', '3'], 'drop_source_columns':0}])
    # data.transform(transform_args=[
    #     {'type':'merge','args':
    #     [{"source_columns":["hah", "test"],"split_flag":"-","drop_source_columns":0},
    #      {"source_columns": ["hah", "merge_hah_test"], "split_flag": "-", "drop_source_columns": 0}]},
    #     {'type':'math','args':
    #     [{'function': 'mean', 'source_columns': ['1', '2', '3'], 'new_column_name': 'ttsdffds'},
    #      {'function': 'min', 'source_columns': ['1', '2', '3'], 'drop_source_columns': 0}]},
    #     {'type': 'split', 'args':
    #         [{"source_column": "hah", "split_flag": "-", "drop_source_column": 0},
    #         {"source_column": "test", "split_flag": "-", "drop_source_column": 1}]}
    # ])
    #print(data.save('20180831_09test'))