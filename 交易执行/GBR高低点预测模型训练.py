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
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split
import dateutil
from pymongo import MongoClient

# 连接数据库并读取数据
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}待训练']
df = pd.DataFrame(list(collection.find()))
df = df.dropna()  # 删除缺失值
# df = df.sort_values(by='timestamp')    # 以日期列为索引,避免计算错误
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '开盘收盘幅',
    f'EMA{9}开盘比值', f'EMA{121}开盘比值',
]
x = df[tezheng]
mubiao = [f'{60}日最高开盘（未来函数）',f'{60}日最低开盘（未来函数）',]
y = df[mubiao]  # 将所有目标变量放在一个DataFrame中

df = df.dropna()  # 删除缺失值
# 将数据集划分训练集和测试集
x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.3, random_state=24)
# 训练集和测试集切片中的随机状态参数会进行随机偏移，取消以后才能指定偏移
model = MultiOutputRegressor(GradientBoostingRegressor(
    loss='squared_error',  # 修改此处 'ls' 为 'squared_error'
    learning_rate=0.1, n_estimators=100, subsample=1.0, criterion='friedman_mse',
    max_depth=5, random_state=24))
# loss：损失函数，支持均方误差（'ls'）、绝对误差（'lad'）等，默认为'ls'。
# learning_rate：学习率，用于控制每个弱分类器的权重更新幅度，默认为0.1。
# n_estimators：弱分类器迭代次数，表示总共有多少个弱分类器组成，也是GBDT的主要超参数之一，默认为100。
# subsample：样本采样比例，用于控制每个弱分类器的训练集采样比例，取值范围在(0,1]之间，默认为1.0，不进行采样。
# criterion：用于衡量节点分裂质量的评价标准，支持平均方差（'mse'）、平均绝对误差（'mae'）、Friedman增益（'friedman_mse'）等。
model.fit(x_train, y_train)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}model.pickle')
# 假设model是我们训练好的模型
with open(file_path, 'wb') as f:
    pickle.dump(model, f)
