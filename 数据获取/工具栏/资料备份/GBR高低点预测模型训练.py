from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
import pickle
from sklearn.metrics import mean_squared_error, r2_score
from datetime import datetime
import os
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split
from pymongo import MongoClient

# 连接数据库并读取数据
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}指标']
df = pd.DataFrame(list(collection.find()))
# 提取数值类型数据
numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
df = df[numerical_cols]
df = df.dropna()  # 删除缺失值
df = df.sort_values(by='timestamp')    # 以日期列为索引,避免计算错误
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '昨日成交额',
]
for n in range(1, 17):  # 计算未来n日涨跌幅
    tezheng += [
        f'{n*10}日最高开盘价比值',
        f'{n*10}日最低开盘价比值',
        f'SMA{n*10}开盘比值',
        f'SMA{n*10}昨日成交额比值',]
x = df[tezheng]
mubiao = ['未来60日最高开盘价', '未来60日最低开盘价', '未来60日最高开盘价日期', '未来60日最低开盘价日期']
y = df[mubiao]
# 将数据集划分训练集和测试集
x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.1, random_state=24)
# 训练集和测试集切片中的随机状态参数会进行随机偏移，取消以后才能指定偏移
model = MultiOutputRegressor(GradientBoostingRegressor(
    loss='quantile', learning_rate=0.01, n_estimators=500, subsample=0.8, max_depth=5, random_state=24))
# loss：损失函数， {'huber', 'squared_error', 'absolute_error', 'quantile'}
# learning_rate：由于分钟级别的K线数据变化较快，建议将学习率设置为较小的值，例如0.01或0.001，从而避免模型过拟合。
# n_estimators：分钟级别的K线数据量较大，模型的训练时间也会相应增加，建议尝试设置较小的值，例如100或500，然后逐步增加直到模型的性能不再提升。
# max_depth：由于分钟级别的K线数据变化较快，建议将每个基模型的深度设置为较小的值，例如3或5，以遏制过拟合的发生。
# subsample：分钟级别的K线数据量较大，建议将子样本随机采样比例设置为较小的值，例如0.8或0.9。
model.fit(x_train, y_train)

y_pred = model.predict(x_test)  # 对模型预测准确率进行统计

# # 设置要插入的数据
# data = {
#     "MAPE": mean_absolute_percentage_error(y_test, y_pred),
#     "MSE": mean_squared_error(y_test, y_pred),
#     "R2_score": r2_score(y_test, y_pred)
# }
# db[f"{name}预测误差"].drop()  # 清空集合中的所有文档
# # 插入数据到 mongodb
# db[f"{name}预测误差"].insert_one(data)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
file_path = os.path.join(dir_path, f'{name}model.pickle')
# 假设model是我们训练好的模型
with open(file_path, 'wb') as f:
    pickle.dump(model, f)
