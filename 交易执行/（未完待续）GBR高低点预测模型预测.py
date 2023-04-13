# 机器学习容易过拟合，一般适合微观层面的研究，宏观上受价值等因素影响有点大
import json
import requests
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
import pickle
from sklearn.metrics import mean_squared_error, r2_score
from datetime import datetime
import os
import pandas as pd
import numpy as np
import akshare as ak
import talib
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
import dateutil
from pymongo import MongoClient


# 连接数据库并读取数据
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}指标']

n=1
# 预测n*60步频后的最高最低区间

df = pd.DataFrame(list(collection.find()))
# 从'model.pickle'文件中加载模型
with open(f'{name}model.pickle', 'rb') as f:
    model = pickle.load(f)

dfwill = df.sort_values(by='timestamp')    # 以日期列为索引,避免计算错误
df = df.dropna()  # 删除缺失值
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '涨跌幅', '开盘收盘幅', '开盘收盘幅',
    f'EMA{9}开盘比值', f'EMA{121}开盘比值',
]

# 获取待训练数据集中最靠后的60个数据
latest_data = dfwill[-60:]

# 获取最后一个数据点的时间戳并转换为datetime格式
last_timestamp = latest_data.iloc[-1]['timestamp']
last_datetime = datetime.fromtimestamp(last_timestamp)

# 构建60日后的日期
future_datetime = last_datetime + dateutil.relativedelta.relativedelta(days=60)

# 构建未来60日的每一天的日期列表
date_list = pd.date_range(start=last_datetime.date(),
                          end=future_datetime.date(), freq='D')

# 构建预测数据集，包括日期和特征
future_data = pd.DataFrame({'timestamp': date_list}).set_index('timestamp')
future_data['最高'] = 0


# 对于每一天的日期，使用模型进行预测
for idx, date in enumerate(date_list):
    # 获取过去60天的数据
    past_data = df[-60 + idx:].reset_index(drop=True)
    # 构建输入特征
    x_future = future_data.iloc[idx][tezheng].values.reshape(1, -1)
    # 使用模型进行预测
    future_data.iloc[idx][f'{n*60}日最高开盘（未来函数）'] = model.predict(x_future)

# 输出预测结果到钉钉
msg = f"{name}未来60天最高价和最低价预测值为：\n"
for date, row in future_data.iterrows():
    msg += f"{date.date()}: 未来最高价：{[f'{n*60}日最高开盘（未来函数）']:.10f}，未来最低价：{row['最低']:.10f}\n"

data = {
    "msgtype": "text",
    "text": {
        "content": msg
    }
}
url = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
headers = {'Content-Type': 'application/json;charset=utf-8'}
r = requests.post(url, headers=headers, data=json.dumps(data))
print(r.content.decode('utf-8'))
