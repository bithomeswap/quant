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

tezheng = [
    # 正相关
    "开盘", "最高", "最低", "收盘",
    "涨跌幅", "40日交易量排名", "是否涨跌停",
    "MACD", "MACDsignal", "MACDhist", "KDJ_K", "KDJ_D", "KDJ_J", "ATR", "wr", "slowk", "slowd",
    # "EMA2成交量比值", "EMA2收盘比值", "EMA2开盘比值", "EMA2最高比值", "EMA2最低比值", "EMA3成交量比值", "EMA3收盘比值", "EMA3开盘比值", "EMA3最高比值", "EMA3最低比值",
    # "EMA4成交量比值", "EMA4收盘比值", "EMA4开盘比值", "EMA4最高比值", "EMA4最低比值", "EMA5成交量比值", "EMA5收盘比值", "EMA5开盘比值", "EMA5最高比值", "EMA5最低比值",
    # "EMA6成交量比值", "EMA6收盘比值", "EMA6开盘比值", "EMA6最高比值", "EMA6最低比值", "EMA7成交量比值", "EMA7收盘比值", "EMA7开盘比值", "EMA7最高比值", "EMA7最低比值",
    # "EMA8成交量比值", "EMA8收盘比值", "EMA8开盘比值", "EMA8最高比值", "EMA8最低比值",
    #  "成交量", "成交额", "成交笔数", "主动买入成交量", "主动买入成交额", "标准时间间隔",
    # 负相关
    "收盘timestamp", "timestamp"
]
x = df[tezheng]

# 数据量太小
n = 2
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


# 假设model是我们训练好的模型
with open(f'{name}model.pickle', 'wb') as f:
    pickle.dump(model, f)
# # 从'model.pickle'文件中加载模型
# with open('model.pickle', 'rb') as f:
#     model = pickle.load(f)
