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
import time
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from pymongo import MongoClient

# 连接数据库并读取数据
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
df = pd.DataFrame(list(db[f'{name}待训练'].find()))
df = df.dropna()  # 删除缺失值
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '开盘收盘幅',
    f'EMA{9}开盘比值', f'EMA{121}开盘比值',
]
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

# 循环获取最新的数据进行预测和保存
while True:
    # 每次循环先将df最新数据读入，然后添加一句print('已经从数据库获取数据')。
    df_latest = pd.DataFrame(list(db[f'{name}待训练'].find()))
    df_latest = df_latest.dropna()  # 删除缺失值
    print('已经从数据库获取数据')
    
    # 判断是否需要进行计算
    if len(df_latest) > len(df):  # 数据有更新
        # 计算价格的方法如下：
        # 对于每个时间戳在60分钟内的数据，找到其中最高价和最低价以及它们出现的时间戳。
        mubiao = []
        for i in range(len(df_latest)):
            time_i = df_latest.iloc[i]['timestamp']
            high_i = df_latest[(df_latest['timestamp'] >= time_i) &
                               (df_latest['timestamp'] <= time_i + 60)].iloc[0]['60日最高开盘（未来函数）']
            low_i = df_latest[(df_latest['timestamp'] >= time_i) &
                              (df_latest['timestamp'] <= time_i + 60)].iloc[0]['60日最低开盘（未来函数）']
            mubiao.append({'time': time_i, 'high': high_i, 'low': low_i})
        # 进行预测
        x = df_latest[tezheng]
        y_pred = model.predict(x)
        
        # 将预测结果保存到mongodb中
        timestamp_list = list(x['timestamp'])
        result_dict = {'timestamp': [pd.to_datetime(ts, unit='s', utc=True) for ts in timestamp_list],
                       'high': y_pred[:, 0],
                       'low': y_pred[:, 1]}
        result_df = pd.DataFrame(result_dict)
        db[f'{name}predictions'].drop()  # 清空集合中的所有文档
        db[f'{name}predictions'].insert_many(result_df.to_dict('records'))

        df = df_latest
        
    # 每隔60s进行一次预测
    time.sleep(60)
