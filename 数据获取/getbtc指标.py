import json
import requests
import pandas as pd
import talib
import os
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = "BTC"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    # 过滤最高价和最低价为负值的数据
    df = df.loc[(df['最高'] >= 0) & (df['最低'] >= 0)]
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

    # 计算昨日振幅
    df['昨日振幅'] = (df.shift(1)['最高']-df.shift(1)['最低'])/df.shift(1)['开盘']
    # 计算昨日成交额
    df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
    # 定义开盘收盘幅
    df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100

    df = df.dropna()  # 删除缺失值，避免无效数据的干扰

    for n in range(2, 11):
        df[f'{n*10}日最高开盘价比值'] = df['开盘']/df['最高'].rolling(n*10).max().shift(1)
        df[f'{n*10}日最低开盘价比值'] = df['开盘']/df['最低'].rolling(n*10).min().shift(1)
        df[f'SMA{n*10}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
        df[f'SMA{n*10}昨日成交额比值'] = df['昨日成交额'] / \
            talib.MA(df['昨日成交额'].values, timeperiod=n*10, matype=0)

        df[f'{n}日最高开盘价比值'] = df['开盘']/df['最高'].rolling(n).max().shift(1)
        df[f'{n}日最低开盘价比值'] = df['开盘']/df['最低'].rolling(n).min().shift(1)
        df[f'SMA{n}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n, matype=0)
        df[f'SMA{n}昨日成交额比值'] = df['昨日成交额'] / \
            talib.MA(df['昨日成交额'].values, timeperiod=n, matype=0)

    for n in range(1, 11):
        df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].shift(-n)/df['收盘']-1

    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码').apply(get_technical_indicators)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))

# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}指标.csv')
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

url = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
headers = {'Content-Type': 'application/json;charset=utf-8'}

data = {
    "msgtype": "text",
    "text": {
        "content": "btc指标计算成功"
    }
}

r = requests.post(url, headers=headers, data=json.dumps(data))
print(r.content.decode('utf-8'))

# import math
# # 获取最后一天的数据
# last_day = grouped.iloc[-1]['日期']
# # print(last_day)

# # 计算总共统计的股票数量
# code_count = len(grouped['代码'].drop_duplicates())
# print(code_count)
# # 筛选出符合条件的股票代码
# for n in range(1, 13):
#     df = df.loc[(df['日期'] == last_day)]
#     df = df[df[f'SMA{n*10}开盘比值'] >= 1].copy()
# # 选取当天'开盘'最低的
# n_top = math.ceil(code_count/50)
# df = df.nsmallest(n_top, '昨日振幅')
# n_top = math.ceil(code_count/500)
# df = df.nsmallest(n_top, '开盘')
# print(df['代码'])
# # url = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
# # headers = {'Content-Type': 'application/json;charset=utf-8'}
# # data = {
# #     "msgtype": "text",
# #     "text": {
# #         "今日符合需求的股票":df['代码'],
# #         "指标详情": df
# #     }
# # }
# # r = requests.post(url, headers=headers, data=json.dumps(data))
# # print(r.content.decode('utf-8'))
