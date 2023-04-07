# 机器学习容易过拟合，一般适合微观层面的研究，宏观上受价值等因素影响有点大
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
# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 选择要分析的产品
name = "BTC"
collection = db[f'{name}待训练']
print('数据库已链接')
df = pd.DataFrame(list(collection.find()))
print('数据获取成功')

# # 之前提取过了
# # 提取数值类型数据
# numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
# df_numerical = df[numerical_cols]

df = df.dropna()  # 删除缺失值，避免无效数据的干扰


# 计算过去n日ema比值指标

tezheng = [
    "timestamp",
    "开盘", "最高", "最低", "收盘", "标准时间间隔",
    "涨跌幅", "是否涨跌停",
    "MACD", "MACDsignal", "MACDhist", "KDJ_K", "KDJ_D", "KDJ_J", "slowk", "slowd"
]

for n in range(2, 10):
    tezheng += [
        f'wr{n*n}', f'ATR{n*n}',
        f'EMA{n*n}最高比值', f'EMA{n*n}最低比值', f'EMA{n*n}开盘比值', f'EMA{n*n}收盘比值', f'EMA{n*n}成交量比值',
        f'SMA{n*n}最高比值', f'SMA{n*n}最低比值', f'SMA{n*n}开盘比值', f'SMA{n*n}收盘比值', f'SMA{n*n}成交量比值',
    ]
x = df[tezheng]
print(x)
# 预测七日后涨跌幅
n = 7
y = df[f'{n}日后总涨跌幅（未来函数）']

# 将数据集划分训练集和测试集
x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.3, random_state=24)

# 训练集和测试集切片中的随机状态参数会进行随机偏移，取消以后才能指定偏移
model = GradientBoostingRegressor(
    loss='squared_error', learning_rate=0.1, n_estimators=100, subsample=1.0, criterion='friedman_mse')
# loss：损失函数，支持均方误差（'ls'）、绝对误差（'lad'）等，默认为'ls'。
# learning_rate：学习率，用于控制每个弱分类器的权重更新幅度，默认为0.1。
# n_estimators：弱分类器迭代次数，表示总共有多少个弱分类器组成，也是GBDT的主要超参数之一，默认为100。
# subsample：样本采样比例，用于控制每个弱分类器的训练集采样比例，取值范围在(0,1]之间，默认为1.0，不进行采样。
# criterion：用于衡量节点分裂质量的评价标准，支持平均方差（'mse'）、平均绝对误差（'mae'）、Friedman增益（'friedman_mse'）等。
model.fit(x_train, y_train)

y_pred = model.predict(x_test)  # 对模型进行预测


# 将测试集的真实值和模型的预测值保存为CSV文件
dfpred = pd.DataFrame({'True values': y_test, 'Predicted values': y_pred,
                      'timestamp': pd.to_datetime(x_test['timestamp'], unit='s', utc=True)})

db[f'{name}predictions'].drop()  # 清空集合中的所有文档
db[f'{name}predictions'].insert_many(dfpred.to_dict('records'))
# 展示时间和timestamp差的八个小时是时区问题，即我本地（本地时间是utf8，东八区北京时间）转换的时候，自动加了八小时


# 设置要插入的数据
data = {
    "MAPE": mean_absolute_percentage_error(y_test, y_pred),
    "MSE": mean_squared_error(y_test, y_pred),
    "R2_score": r2_score(y_test, y_pred)
}
db[f"{name}预测误差"].drop()  # 清空集合中的所有文档
# 插入数据到 mongodb
db[f"{name}预测误差"].insert_one(data)

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
# # 从'model.pickle'文件中加载模型
# with open('model.pickle', 'rb') as f:
#     model = pickle.load(f)
