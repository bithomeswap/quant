# 导入必要的库
import numpy as np
import pandas as pd
import talib
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error
from datetime import datetime, timedelta
from pymongo import MongoClient

n = 60  # 预测未来60分钟范围内的整体高低点
window_size = 12  # 滚动窗口大小

# 连接数据库并读取数据
client = MongoClient(
    'mongodb://YourDBUsername:YourDBPassword@YourDBIP:27017/YourDBName?authSource=YourAuthSource')
db = client['YourDBName']
name = 'BTC'
collection = db[f'{name}待训练']
df = pd.DataFrame(list(collection.find()))
df = df.dropna()  # 删除缺失值

tezheng = [
    'timestamp',
    "涨跌幅", '开盘幅',
]
for n in range(2, 12):
    tezheng += [
        f'EMA{n*n}最高比值', f'EMA{n*n}最低比值', f'EMA{n*n}开盘比值', f'EMA{n*n}收盘比值',
    ]
# 计算技术指标

df[f'{n}日最高开盘（未来函数）'] = df['开盘'].rolling(-20).max()
df[f'{n}日最高开盘（未来函数）'] = df['开盘'].rolling(-20).min()

# 计算滚动窗口内的整体高低点


def get_high_low(df, n):
    ts = df['timestamp'].iloc[-1]
    start_ts = ts - timedelta(minutes=60)
    end_ts = ts + timedelta(minutes=n)

    sub_df = df[(df['timestamp'] >= start_ts) & (df['timestamp'] <= end_ts)]
    high_point = sub_df['最高价'].max()
    low_point = sub_df['最低价'].min()

    return high_point, low_point


df['整体高点'] = np.nan
df['整体低点'] = np.nan
for i in range(window_size, len(df)):
    high_point, low_point = get_high_low(df[:i], n)
    df.at[i, '整体高点'] = high_point
    df.at[i, '整体低点'] = low_point

# 提取特征和前n期涨跌幅作为标签
x = df[tezheng]
y = df[f'{n}日后总涨跌幅（未来函数）']

# 切分训练集和测试集
train_size = int(len(df) * 0.7)
x_train, x_test = x[:train_size], x[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# 建立模型并训练
model = GradientBoostingRegressor(
    loss='squared_error', learning_rate=0.1, n_estimators=1000, subsample=1.0, criterion='friedman_mse')
model.fit(x_train, y_train)

# 预测未来60分钟范围内的整体高低点
last_record = list(collection.find().sort('_id', -1).limit(1))[0]
last_timestamp = last_record['timestamp']
start_ts = last_timestamp + 60
end_ts = start_ts + timedelta(minutes=n)
data = list(collection.find({'timestamp': {"$gte": start_ts, "$lte": end_ts}}))
new_df = pd.DataFrame(data)
new_df = new_df.dropna()

# 获取新数据集的特征
new_df['20日最高价'] = new_df['收盘价'].rolling(20).max()
new_df['20日最低价'] = new_df['收盘价'].rolling(20).min()
new_df['20日区间'] = (new_df['20日最高价'] - new_df['20日最低价']) / new_df['20日最高价']
new_x = new_df[tezheng]

# 预测涨跌幅并更新模型
new_y_pred = model.predict(new_x)
new_y_true = new_df['未来N日涨跌幅'].values
new_mape = mean_absolute_percentage_error(new_y_true, new_y_pred)
if new_mape < 1000:
    model.fit(x, y)

# 将预测结果保存到数据库
new_timestamps = new_df['timestamp'].values
new_high_points = np.zeros_like(new_timestamps)
new_low_points = np.zeros_like(new_timestamps)
for i in range(len(new_timestamps)):
    high_point, low_point = get_high_low(df, n)
    new_high_points[i] = high_point
    new_low_points[i] = low_point

results = {
    'timestamp': new_timestamps,
    '整体高点': new_high_points,
    '整体低点': new_low_points,
}
db[f'{name}预测结果'].insert_many(results.to_dict('records'))
