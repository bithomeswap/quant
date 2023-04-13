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
x = df[tezheng]
print(x)
mubiao = []
# for n in range(1, 9):
#     mubiao += [
#         f'{n*60}日最高开盘（未来函数）',
#     ]
n = 1
y = df[f'{n*60}日最高开盘（未来函数）']

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
