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
# name = 'COIN'
# name = 'STOCK'
# name = 'BTC'
name = '指数'
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    try:
        # 删除最高价和最低价为负值的数据
        df.drop(df[(df['最高'] < 0) | (df['最低'] < 0)].index, inplace=True)
        df.sort_values(by='日期', inplace=True)    # 以日期列为索引,避免计算错误

        # 计算昨日振幅
        df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100
        # 计算昨日振幅
        df['昨日振幅'] = (df.shift(1)['最高']-df.shift(1)['最低'])/df.shift(1)['开盘']
        # 计算昨日涨跌幅
        df['昨日涨跌幅'] = df.shift(1)['涨跌幅']
        # 计算昨日成交额
        df['昨日成交额'] = df.shift(1)['成交额']
        # 计算昨日资金贡献
        df['昨日资金贡献'] = df['昨日涨跌幅'] / df['昨日成交额']
        # 定义开盘收盘幅
        df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
        df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        # 固定周期指标
        df[f'{9}周期开盘rsi'] = talib.RSI(df['开盘'], timeperiod=9)
        df[f'{9}日最高昨日成交额比值'] = df['昨日成交额']/df['昨日成交额'].rolling(9).max()
        df[f'{20}日最低昨日成交额比值'] = df['昨日成交额'] / df['昨日成交额'].rolling(20).min()
        df[f'SMA{9}昨日涨跌幅'] = talib.MA(
            df['昨日涨跌幅'].values, timeperiod=9, matype=0)
        df[f'SMA{9}昨日振幅'] = talib.MA(
            df['昨日涨跌幅'].values, timeperiod=9, matype=0)
        for n in range(1, 7):
            # 定义长周期比值的均值
            df[f'SMA{n*5}昨日成交额比值'] = df['昨日成交额'] / \
                talib.MA(df['昨日成交额'].values, timeperiod=n*5, matype=0)
            df[f'SMA{n*5}开盘比值'] = df['开盘'] / \
                talib.MA(df['开盘'].values, timeperiod=n*5, matype=0)
            # 定义昨日资金贡献rsi
            df[f'{n*2}周期昨日资金贡献rsi'] = talib.RSI(df['昨日资金贡献'], timeperiod=n*2)
            # 定义昨日成交额rsi
            df[f'{n*2}周期昨日成交额rsi'] = talib.RSI(df['昨日成交额'], timeperiod=n*2)
            # 最低最高距离指标(最高看短周期,最低看长周期),判断临近新高新低的位置
            df[f'{n*2}日最高开盘比值'] = df['开盘'] / df['开盘'].rolling(n*2).max()
            df[f'{n*2}日最低开盘比值'] = df['开盘'] / df['开盘'].rolling(n*2).min()
        for n in range(1, 20):
            df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].shift(-n) / df['收盘'] - 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码', group_keys=False).apply(get_technical_indicators)


def paiming(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if ('未来函数' not in str(column)) & ('收盘' != str(column)) & ('最高' != str(column)) & ('最低' != str(column)) & ('成交量' != str(column)) & ('代码' != str(column)) & ('开盘' != str(column)):
            df = pd.concat([df, (df[str(column)].rank(
                method='max', ascending=False) / len(df)).rename(f'{str(column)}_rank')], axis=1)
    return df


# 分组并计算指标排名
grouped = grouped.groupby(['日期'], group_keys=False).apply(paiming)


# # 今日筛选股票推送(多头)
# df = grouped.sort_values(by='日期')
# # 获取最后一天的数据
# last_day = df.iloc[-1]['日期']
# # 计算总共统计的股票数量
# code_count = len(df['代码'].drop_duplicates())
# df = df[df[f'日期'] == last_day].copy()
# # 成交额过滤劣质股票
# df = df[df[f'昨日成交额'] >= 20000000].copy()

# # 开盘价过滤高滑点股票
# df = df[df[f'开盘'] >= 0.01].copy()
# print(len(df))
# # 发布到钉钉机器人
# df['市场'] = name
# message = df[['市场', '代码', '日期', '开盘', '昨日振幅']].to_markdown()
# print(type(message))
# webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
# requests.post(webhook, json={'msgtype': 'markdown', 'markdown': {
#               'title': 'DataFrame', 'text': message}})


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
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
new_collection.drop()  # 清空集合中的所有文档
# 将数据分批插入
batch_size = 5000  # 批量插入的大小
num_batches = len(grouped) // batch_size + 1
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = (i + 1) * batch_size
    data_slice = grouped[start_idx:end_idx]
    new_collection.insert_many(data_slice.to_dict('records'))
