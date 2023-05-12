import tradelist
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
# 设置参数
# name = '分钟COIN'
# name = 'COIN'
name = '分钟上证'
# name = '上证'
# name = '分钟深证'
# name = '深证'
# name = '分钟ETF'
# name = 'ETF'

collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    try:
        df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        # 删除最高价和最低价为负值的数据
        df.drop(df[(df['最高'] < 0) | (df['最低'] < 0)].index, inplace=True)
        df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
        # 定义开盘收盘幅
        df['开盘收盘幅'] = df['开盘']/df['收盘'].copy().shift(1) - 1
        # 计算涨跌幅
        df['涨跌幅'] = df['收盘']/df['收盘'].copy().shift(1) - 1
        # 计算昨日振幅
        df['昨日振幅'] = (df['最高'].copy().shift(
            1)-df['最低'].copy().shift(1))/df['开盘'].copy().shift(1)
        # 计算昨日成交额
        df['昨日成交额'] = df['成交额'].copy().shift(1)
        # 计算昨日涨跌
        df['昨日涨跌'] = df['涨跌幅'].copy().shift(1)+1
        # 计算昨日资金贡献
        df['昨日资金贡献'] = df['昨日涨跌'] / df['昨日成交额']
        # 计算昨日资金波动
        df['昨日资金波动'] = df['昨日振幅'] / df['昨日成交额']
        # 计算昨日资金贡献
        df['昨日资金成本'] = df['昨日涨跌'] * df['昨日成交额']
        for n in range(2, 10):
            df[f'过去{n}日总涨跌'] = df['开盘']/(df['开盘'].copy().shift(n))
            df[f'过去{n*5}日总涨跌'] = df['开盘']/(df['开盘'].copy().shift(n*5))
        for n in range(1, 20):
            df[f'{n}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-n) / df['收盘']) - 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码', group_keys=False).apply(get_technical_indicators)


def paiming(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if '未来函数' not in str(column):
            df = pd.concat([df, (df[str(column)].rank(
                method='max', ascending=False) / len(df)).rename(f'{str(column)}_rank')], axis=1)
    return df


# 分组并计算指标排名
grouped = grouped.groupby(['日期'], group_keys=False).apply(paiming)

tradelist.tradelist(grouped, name)

# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
new_collection.drop()  # 清空集合中的所有文档
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f'{name}指标.csv')
grouped.to_csv(file_path, index=False)
print('准备插入数据')
# 将数据分批插入
batch_size = 5000  # 批量插入的大小
num_batches = len(grouped) // batch_size + 1
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = (i + 1) * batch_size
    data_slice = grouped[start_idx:end_idx]
    new_collection.insert_many(data_slice.to_dict('records'))
