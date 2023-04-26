# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import json
import requests
import pandas as pd
import talib
import math
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = "实盘STOCK"
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
    for n in range(1, 13):
        df[f'{n*10}日最高开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).max()
        df[f'{n*10}日最低开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).min()
        df[f'SMA{n*10}开盘比值'] = df['开盘'] /talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
    return df


# 按照“代码”列进行分组并计算技术指标
df = data.groupby('代码').apply(get_technical_indicators)
# print(type(df))

# 获取最后一天的数据
last_day = df.iloc[-1]['日期']
# print(last_day)

# 计算总共统计的股票数量
code_count = len(df['代码'].drop_duplicates())
print(code_count)

# 筛选出符合条件的股票代码
for n in range(1, 13):  
    df = df.loc[(df['日期'] == last_day)]
    df = df[df[f'SMA{n*10}开盘比值'] >= 1].copy()
# 选取当天'开盘'最低的
n_top = math.ceil(code_count/50)
df = df.nsmallest(n_top, '昨日振幅')
n_top = math.ceil(code_count/500)
df = df.nsmallest(n_top, '开盘')
print(df['代码'])

# url = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
# headers = {'Content-Type': 'application/json;charset=utf-8'}
# data = {
#     "msgtype": "text",
#     "text": {
#         "今日符合需求的股票":df['代码'],
#         "指标详情": df
#     }
# }
# r = requests.post(url, headers=headers, data=json.dumps(data))
# print(r.content.decode('utf-8'))
