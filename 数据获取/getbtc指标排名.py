import pytz
import datetime
import math
import requests
import pandas as pd
import talib
import os
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']

# name = 'COIN'
# name = 'STOCK'
name = 'BTC'
# name = '指数'

collection = db[f'{name}指标']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


# 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
def paiming(df):  # 定义计算技术指标的函数
    # 计算每个指标的排名
    for n in range(2, 11):
        col_name = f'SMA{n*10}开盘比值'
        df[f'{col_name}_rank'] = df[col_name].rank(
            method='max', ascending=False)
        df[f'{col_name}_rank'] = df[f'{col_name}_rank'] / len(df)
        col_name = f'SMA{n*10}昨日成交额比值'
        df[f'{col_name}_rank'] = df[col_name].rank(
            method='max', ascending=False)
        df[f'{col_name}_rank'] = df[f'{col_name}_rank'] / len(df)
        col_name = f'SMA{n}开盘比值'
        df[f'{col_name}_rank'] = df[col_name].rank(
            method='max', ascending=False)
        df[f'{col_name}_rank'] = df[f'{col_name}_rank'] / len(df)
        col_name = f'SMA{n}昨日成交额比值'
        df[f'{col_name}_rank'] = df[col_name].rank(
            method='max', ascending=False)
        df[f'{col_name}_rank'] = df[f'{col_name}_rank'] / len(df)
    # 计算昨日振幅的排名(升序)
    df['昨日振幅_rank'] = df['昨日振幅'].rank(method='max')
    df['昨日振幅_rank'] = df['昨日振幅_rank'] / len(df)
    # 计算昨日成交额的排名(升序)
    df['昨日成交额_rank'] = df['昨日成交额'].rank(method='max')
    df['昨日成交额_rank'] = df['昨日成交额_rank'] / len(df)
    # 计算昨日涨跌幅的排名(升序)
    df['开盘收盘幅_rank'] = df['开盘收盘幅'].rank(method='max')
    df['开盘收盘幅_rank'] = df['开盘收盘幅_rank'] / len(df)
    # 计算开盘价的排名(升序)
    df['开盘_rank'] = df['开盘'].rank(method='max')
    df['开盘_rank'] = df['开盘_rank'] / len(df)
    return df


# 分组并计算指标排名
grouped = data.groupby(['日期'], group_keys=False).apply(paiming)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}指标排名.csv')
grouped.to_csv(file_path, index=False)
print('准备插入数据')
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标排名']
new_collection.drop()  # 清空集合中的所有文档
# 将数据分批插入
batch_size = 5000  # 批量插入的大小
num_batches = len(grouped) // batch_size + 1
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = (i + 1) * batch_size
    data_slice = grouped[start_idx:end_idx]
    new_collection.insert_many(data_slice.to_dict('records'))
