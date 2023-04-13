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
import os
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = "STOCK"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
    # 成交量变成浮点数
    df['成交量'] = df['成交量'].astype(float)
    # 定义开盘幅
    df['开盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
    # 定义收盘幅即涨跌幅
    df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100

    # 计算过去n日ema比值指标
    for n in range(2, 12):
        df[f'EMA{n*n}收盘比值'] = df['收盘'] / \
            talib.MA(df['收盘'].values, timeperiod=n*n, matype=0)
        df[f'EMA{n*n}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n*n, matype=0)
        df[f'EMA{n*n}最高比值'] = df['最高'] / \
            talib.MA(df['最高'].values, timeperiod=n*n, matype=0)
        df[f'EMA{n*n}最低比值'] = df['最低'] / \
            talib.MA(df['最低'].values, timeperiod=n*n, matype=0)

        df[f'EMA{n}收盘比值'] = df['收盘'] / \
            talib.MA(df['收盘'].values, timeperiod=n, matype=0)
        df[f'EMA{n}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n, matype=0)
        df[f'EMA{n}最高比值'] = df['最高'] / \
            talib.MA(df['最高'].values, timeperiod=n, matype=0)
        df[f'EMA{n}最低比值'] = df['最低'] / \
            talib.MA(df['最低'].values, timeperiod=n, matype=0)

    # 计算过去n日ema比值指标
    for n in range(2, 8):
        df[f'EMA9收盘动能{n}'] = df[f'EMA{n}收盘比值']/df[f'EMA9收盘比值']
        df[f'EMA9开盘动能{n}'] = df[f'EMA{n}开盘比值']/df[f'EMA9开盘比值']
        df[f'EMA9最高动能{n}'] = df[f'EMA{n}最高比值']/df[f'EMA9最高比值']
        df[f'EMA9最低动能{n}'] = df[f'EMA{n}最低比值']/df[f'EMA9最低比值']

    df = df.dropna()  # 删除缺失值，避免无效数据的干扰
    for n in range(1, 10):  # 计算未来n日涨跌幅
        df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].pct_change(n).shift(-n)*100
        df[f'{n}日最高开盘（未来函数）'] = df['开盘'].rolling(-20).max()
        df[f'{n}日最高开盘（未来函数）'] = df['开盘'].rolling(-20).min()

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
        "content": "stock指标计算成功"
    }
}

r = requests.post(url, headers=headers, data=json.dumps(data))
print(r.content.decode('utf-8'))
