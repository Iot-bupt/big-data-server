import pandas as pd
from sqlalchemy import create_engine

def get_tables_pandas(engine):
    tables = pd.read_sql_query('show tables', engine)
    for table_name in [tables.iloc[i, 0] \
                       for i in range(tables.shape[0])]:
        table = {'table_name': table_name}
        sql = 'desc %s' % table_name
        each_table = pd.read_sql_query(sql, engine)
        table['cloumns'] = [each_table.iloc[j, 0] \
                            for j in range(each_table.shape[0])]
        yield table

class Data():
    def __init__(self,
                 source,
                 source_engine,
                 target=None,
                 target_engine=None,
                 args=None):
        self.args = args
        self.source_engine = source_engine
        if target_engine is None:
            self.target_engine = source_engine
        else:
            self.target_engine = target_engine
        self.source = source
        self.target = target
        self.df = pd.read_sql(self.source,
                              self.source_engine)

    def filter(self, filter_args=None):
        if filter_args is None:
            filter_args = self.args.get('filter', [])
        for item in filter_args:
            func = self.get_filter(item['cmp'], item['value'])

            self.df = self.df[func(self.df[item['column']])]

    def drop(self, drop_args=None):
        if drop_args is None:
            drop_args = self.args.get('drop', [])
        self.df.drop(drop_args,
                     axis=1,
                     inplace=True)

    def dropna(self, dropna_args=None):
        if dropna_args is None:
            dropna_args = self.args.get('dropna', [])
        self.df.dropna(subset=dropna_args,
                       inplace=True)

    def fillna(self, fillna_args=None):
        values = {}
        mean = self.df.mean()
        median = self.df.median()
        mode = self.df.mode().iloc[0]
        if fillna_args is None:
            fillna_args = self.args.get('fillna', [])
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

    def etl(self, target=None, target_engine=None, args=None):
        if args is None:
            args = self.args
        if 'drop' in args:
            self.drop(args.get('drop', []))
        if 'dropna' in args:
            self.dropna(args.get('dropna', []))
        if 'filter' in args:
            self.filter(args.get('filter', []))
        if 'fillna' in args:
            self.fillna(args.get('fillna', []))
        if target is None:
            target = self.target
        if target_engine is None:
            target_engine = self.target_engine

        self.save(target, target_engine)


    def save(self, target=None, target_engine=None):
        if target is None:
            target = self.target
        if target_engine is None:
            target_engine = self.target_engine
        self.df.to_sql(name=target,
                       con=target_engine,
                       if_exists='append',
                       index=False)

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
            if cmp == 'in':
                return value in x
            if cmp == 'not in':
                return value not in x
            return True
        return _filter

