from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
import pickle
import pandas as pd
import numpy as np
import akshare as ak
import talib
from sklearn.ensemble import GradientBoostingRegressor
from pymongo import MongoClient
import requests
import os
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}待训练']
# 读取最新60个文档
df = pd.DataFrame(
    list(collection.find().sort([("timestamp", -1)]).limit(60)))
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))
# 读取模型
model_file_path = os.path.join(parent_dir_path, f'{name}model.pickle')
with open(model_file_path, 'rb') as f:
    model = pickle.load(f)
# 确认特征列
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '开盘收盘幅',
    f'SMA{9}开盘比值', f'SMA{121}开盘比值', f'SMA{9}开盘动能{4}',
]
# 对于每个时间戳在60分钟内的数据，找到其中最高价和最低价以及它们出现的时间戳。
mubiao = ['60日最高开盘价', '60日最低开盘价', '最高开盘价日期', '最低开盘价日期']
# 进行预测
x = df[tezheng]
y_pred = model.predict(x)
# 提取预测结果
print(type(y_pred))
# 假设 y_pred 是一个 numpy 数组，将其转换为 DataFrame
# df_pred = pd.DataFrame(y_pred)
# df_pred.to_csv('df_pred.csv')

predictions = pd.DataFrame({
    'timestamp': df['timestamp'],
    '60日最高开盘价': y_pred[:, 0],
    '60日最低开盘价': y_pred[:, 1],
    '最高开盘价日期': y_pred[:, 2],
    '最低开盘价日期': y_pred[:, 3],
})

# 发布到钉钉机器人
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'

for i in range(len(predictions)):
    message = f"产品名称：{name}\n预测日期：{predictions['timestamp'][i]}\n'60日最高开盘价':{predictions['60日最高开盘价'][i]}\n'60日最低开盘价':{predictions['60日最低开盘价'][i]}\n'最高开盘价日期':{predictions['最高开盘价日期'][i]}\n'最低开盘价日期':{predictions['最低开盘价日期'][i]}"
    print(message)
    requests.post(webhook, json={
        'msgtype': 'text',
        'text': {
            'content': message
        }})
