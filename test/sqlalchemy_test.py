import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:root@172.24.32.169:3306/BUPT_IOT')

# df = pd.DataFrame({'id':[1,None,3,None],'num':[None,34,None,89]})
# # 将新建的DataFrame储存为MySQL中的数据表，不储存index列
# df.to_sql(name='mydf', con=engine, if_exists='append',index= False)

df = pd.read_sql('mydf', engine)
print(df)
# sql = 'select * from mydf'
# sql = 'show tables'
# sql = 'desc app'
# df = pd.read_sql_query(sql, engine)

def get_tables(engine):
    tables = pd.read_sql_query('show tables', engine)
    for table_name in [tables.iloc[i, 0] for i in range(tables.shape[0])]:
        table = {'table_name': table_name}
        sql = 'desc %s' % table_name
        each_table = pd.read_sql_query(sql, engine)
        table['cloumns'] = [each_table.iloc[j, 0] for j in range(each_table.shape[0])]
        yield table

def get_filter(cmp, value):
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

#
# #过滤
# f = get_filter('=')
# df = df[f(df['id'])]

# # 删除指定列
# df.drop(['id','index'], axis=1, inplace=True)

# #去空值
# df.dropna(subset=['index', 'id', 'num'], inplace=True)

#缺失值填充
mean = df.mean()
median = df.median()
mode = df.mode().iloc[0]
print(mean['id'], median['id'], mode['id'])
df.fillna(mode, inplace=True)
#id
# df['test'] = df['id'].map(str) + '-' + df['num'].map(str)
# print(df)
# import json
# print(json.dumps(list(get_tables(engine))))
from numpy import nan as NaN
df1=pd.DataFrame([[1,2,3],[NaN,NaN,2],[NaN,NaN,NaN],[8,8,NaN]])
print(df1.fillna({0:10,1:20,2:'hah'}))
from etl.data import Data
args = {
        # 'filter':[{'column':'id', 'cmp':'>=', 'value':3},
        #           {'column': 'num', 'cmp': '<', 'value': 89}],
        #'drop':['index', 'id'],
        #'dropna':['index'],
        'fillna':[{'column':'index', 'value':'mean'},
                  {'column': 'id', 'value': 'mode'},
                  {'column': 'num', 'value': 10000},]}
data = Data('mydf', engine, args=args)
data.etl(target='test')
print(data.df)
