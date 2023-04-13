from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
import pickle
import pandas as pd
import numpy as np
import akshare as ak
import talib
from sklearn.ensemble import GradientBoostingRegressor
from pymongo import MongoClient
import requests

client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}待训练']
# 读取最新60个文档
data = pd.DataFrame(
    list(collection.find().sort([("timestamp", -1)]).limit(60)))

# 计算过去n日ema比值指标
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '开盘收盘幅',
    f'EMA{9}开盘比值', f'EMA{121}开盘比值',
]
x_test = data[tezheng]

# 加载模型
with open(f'{name}model.pickle', 'rb') as f:
    model = pickle.load(f)

# 进行预测
y_pred = model.predict(x_test)

# 提取预测结果
predictions = pd.DataFrame({
    'timestamp': data['timestamp'],
    'predicted_high': y_pred
})

# 保存预测结果到数据库
db[f'{name}predictions'].drop()  # 清空集合中的所有文档
db[f'{name}predictions'].insert_many(predictions.to_dict('records'))

# 发布到钉钉机器人
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'

for i in range(len(predictions)):
    message = f"产品名称：{name}\n预测日期：{predictions['timestamp'][i]}\n预测60分钟后最高值：{predictions['predicted_high'][i]}"
    requests.post(webhook, json={
        'msgtype': 'text',
        'text': {
            'content': message
        }
    })
