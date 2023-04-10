# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import pandas as pd
import talib
import os
from pymongo import MongoClient

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']  # 将dbname替换成实际的数据库名称
name = "BTC"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
df = pd.DataFrame(list(collection.find()))
print("数据读取成功")

df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

# 定义开盘幅
df['开盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
# 定义收盘幅即涨跌幅
df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100

# 计算趋势确认指标MACD指标
macd, macdsignal, macdhist = talib.MACD(
    df['收盘'].values, fastperiod=12, slowperiod=26, signalperiod=9)
df['MACD'] = macd
df['MACDsignal'] = macdsignal
df['MACDhist'] = macdhist

# 将MACD指标和MACD信号线转换为Pandas中的Series对象
macd = pd.Series(macd, index=df.index)
macdsignal = pd.Series(macdsignal, index=df.index)
macdhist = pd.Series(macdhist, index=df.index)

# 判断金叉和死叉的条件
cross_up = (macd > macdsignal) & (macd.shift(1) < macdsignal.shift(1))  # 金叉
cross_down = (macd < macdsignal) & (macd.shift(1) > macdsignal.shift(1))  # 死叉

# 判断低位金叉和高位金叉的条件
low_cross_up = cross_up & (macd < 0)  # 低位金叉，MACD指标在零轴以下
high_cross_up = cross_up & (macd >= 0)  # 高位金叉，MACD指标在零轴以上

# 判断低位死叉和高位死叉的条件
low_cross_down = cross_down & (macd > 0)  # 低位死叉，MACD指标在零轴以上
high_cross_down = cross_down & (macd <= 0)  # 高位死叉，MACD指标在零轴以下

# 将结果保存在一列中
df['MACD交叉状态'] = 0  # 先初始化为0，表示其他情况
df.loc[low_cross_up, 'MACD交叉状态'] = -1
df.loc[low_cross_down, 'MACD交叉状态'] = -2
df.loc[high_cross_up, 'MACD交叉状态'] = 1
df.loc[high_cross_down, 'MACD交叉状态'] = 2

# 计算过去n日ema比值指标
for n in range(2, 12):
    df[f'EMA{n*n}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}开盘比值'] = df['开盘'] / \
        talib.MA(df['收盘'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}最高比值'] = df['最高'] / \
        talib.MA(df['收盘'].values, timeperiod=n*n, matype=0)
    df[f'EMA{n*n}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=n*n, matype=0)

    df[f'SMA{n*n}收盘比值'] = df['收盘'] / \
        talib.SMA(df['收盘'].values, timeperiod=n*n)
    df[f'SMA{n*n}开盘比值'] = df['开盘'] / \
        talib.SMA(df['开盘'].values, timeperiod=n*n)
    df[f'SMA{n*n}最高比值'] = df['最高'] / \
        talib.SMA(df['最高'].values, timeperiod=n*n)
    df[f'SMA{n*n}最低比值'] = df['最低'] / \
        talib.SMA(df['最低'].values, timeperiod=n*n)

df = df.dropna()  # 删除缺失值，避免无效数据的干扰
for n in range(1, 20):  # 计算未来n日涨跌幅
    df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].pct_change(n).shift(-n)*100

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))

# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}指标.csv')
df.to_csv(file_path, index=False)
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
records = df.to_dict("records")  # 将DataFrame转换为字典
new_collection.drop()  # 清空集合中的所有文档
new_collection.insert_many(records)  # 插入数据到MongoDB数据库中的新的集合中
