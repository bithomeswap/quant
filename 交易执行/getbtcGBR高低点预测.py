from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import talib
from datetime import datetime, timedelta
from pymongo import MongoClient
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']

# 选择要分析的产品
name = '上证指数'
# 获取历史数据
collection = db[f'{name}']
df = pd.DataFrame(list(collection.find()))
print("数据读取成功")
df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
# 成交量变成浮点数
df['成交量'] = df['成交量'].astype(float)
# 定义开盘幅
df['开盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
# 定义收盘幅即涨跌幅
df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100

# 计算过去n日ema比值指标
for n in range(2, 12):
    df[f'EMA{n*n}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}最高比值'] = df['最高'] / \
        talib.MA(df['最高'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=n, matype=0)
    df[f'EMA{n}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=n, matype=0)
    df[f'EMA{n}最高比值'] = df['最高'] / \
        talib.MA(df['最高'].values, timeperiod=n, matype=0)
    df[f'EMA{n}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=n, matype=0)
    # 计算过去n日ema比值指标
for n in range(2, 8):
    df[f'EMA9收盘动能{n}'] = df[f'EMA{n}收盘比值']/df[f'EMA9收盘比值']
    df[f'EMA9开盘动能{n}'] = df[f'EMA{n}开盘比值']/df[f'EMA9开盘比值']
    df[f'EMA9最高动能{n}'] = df[f'EMA{n}最高比值']/df[f'EMA9最高比值']
    df[f'EMA9最低动能{n}'] = df[f'EMA{n}最低比值']/df[f'EMA9最低比值']

df = df.dropna()  # 删除缺失值，避免无效数据的干扰
# 特征工程
features = [
    'timestamp',
    "涨跌幅", '开盘幅',]
for n in range(2, 12):
    features += [
        f'EMA{n*n}最高比值', f'EMA{n*n}最低比值', f'EMA{n*n}开盘比值', f'EMA{n*n}收盘比值',
    ]

data = df[features]
data = data.dropna()

# 定义预测函数
def predict_high_low(data, next_minutes=60, test_size=0.3, random_state=24):
    # 预处理数据
    X = data[:-next_minutes]
    y = data['最高'][next_minutes:]

    # 将数据集划分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state)

    # 训练模型
    model = GradientBoostingRegressor(
        loss='ls', learning_rate=0.1, n_estimators=100, criterion='friedman_mse')
    model.fit(X_train, y_train)

    # 对测试集进行预测
    y_pred = model.predict(X_test)

    # 计算预测误差
    mape = mean_absolute_error(y_test, y_pred) / np.mean(y_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f'MAPE: {mape:.4f}, MSE: {mse:.4f}, R2: {r2:.4f}')

    # 对未来60分钟进行预测
    last_data = data.tail(1)
    for i in range(next_minutes):
        pred_data = last_data.copy()
        pred_data = pred_data.drop(columns=['最高价', '最低价'])

        # 对该分钟的数据进行预测
        pred_high = model.predict(pred_data)[0]
        pred_low = last_data['最低价'].values[0]

        # 将预测值填入到数据集中
        now = datetime.strptime(last_data.index[-1], '%Y-%m-%d %H:%M:%S')
        next_time = now + timedelta(minutes=1)
        next_data = last_data.copy()
        next_data.loc[next_time] = [last_data['开盘价'].values[0], pred_high, pred_low, last_data['收盘价'].values[0],
                                    np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
        last_data = next_data

    return last_data.tail(next_minutes)


# 使用上证指数的数据进行预测
collection_name = f'{name}待训练'
collection = db[collection_name]
data = pd.DataFrame(list(collection.find()))
data = data.set_index('timestamp')
data = data[['开盘价', '最高价', '最低价', '收盘价']]
data.index = pd.to_datetime(data.index, unit='s', utc=True)
next_minutes = 60

# 对未来60分钟进行预测
result = predict_high_low(data, next_minutes=next_minutes)

# 打印预测结果
print(result)
