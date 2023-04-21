# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import pandas as pd
import talib
import os
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']  # 将dbname替换成实际的数据库名称
name = "上证指数"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
df = pd.DataFrame(list(collection.find()))
print("数据读取成功")
# 过滤最高价和最低价为负值的数据
df = df.loc[(df['最高'] >= 0) & (df['最低'] >= 0)]
df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

# 计算昨日成交额
df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
# 定义开盘收盘幅
df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
# 定义收盘幅即涨跌幅
df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100

df = df.dropna()  # 删除缺失值，避免无效数据的干扰

df['未来60日最高开盘价'] = df['开盘'].rolling(60).max().shift(-60)
df['未来60日最低开盘价'] = df['开盘'].rolling(60).min().shift(-60)
df['未来60日最高开盘价日期'] = df['开盘'].rolling(60).apply(
    lambda x: x.argmax(), raw=True).shift(-60)
df['未来60日最低开盘价日期'] = df['开盘'].rolling(60).apply(
    lambda x: x.argmin(), raw=True).shift(-60)

for n in range(1, 17):  # 计算未来n日涨跌幅
    df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].pct_change(n).shift(-n)*100
    df[F'{n*10}日最高开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).max()
    df[F'{n*10}日最低开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).min()
    df[f'SMA{n*10}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
    df[f'SMA{n*10}昨日成交额比值'] = df['昨日成交额'] / \
        talib.MA(df['昨日成交额'].values, timeperiod=n*10, matype=0)

file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))

# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}指标.csv')
df.to_csv(file_path, index=False)
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
records = df.to_dict("records")  # 将DataFrame转换为字典
new_collection.drop()  # 清空集合中的所有文档
new_collection.insert_many(records)  # 插入数据到MongoDB数据库中的新的集合中
