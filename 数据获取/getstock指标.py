# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import pandas as pd
import talib
import numpy as np
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = "STOCK"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

    # 定义涨跌幅
    df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100
    # 定义振幅
    df['振幅'] = ((df['最高']-df['最低'])/df['开盘'])*100
    # 计算开盘后标准时间间隔
    df['标准时间间隔'] = pd.to_datetime(df['日期']) - pd.to_datetime(df.iloc[0]['日期'])
    df['标准时间间隔'] = df['标准时间间隔'].dt.total_seconds().astype(int)

    if len(df) >= 41:
        df['40日交易量排名'] = df.loc[:, '成交量'].rolling(window=41, min_periods=1).apply(
            lambda x: pd.Series(x).rank(ascending=False).iloc[-1] if len(x) >= 41 else np.nan)
    else:
        df['40日交易量排名'] = np.nan

    # 是否涨跌停
    df.loc[df['涨跌幅'] > 9.9, '是否涨跌停'] = 1
    df.loc[df['涨跌幅'] < -9.9, '是否涨跌停'] = -1
    df.loc[(df['涨跌幅'] >= -9.9) & (df['涨跌幅'] <= 9.9), '是否涨跌停'] = 0

    # 计算趋势确认指标MACD指标
    macd, macdsignal, macdhist = talib.MACD(
        df['收盘'].values, fastperiod=12, slowperiod=26, signalperiod=9)
    df['MACD'] = macd
    df['MACDsignal'] = macdsignal
    df['MACDhist'] = macdhist

    # 计算行情过滤指标KDJ指标
    high, low, close = df['最高'].values, df['最低'].values, df['收盘'].values
    k, d = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3,
                       slowk_matype=0, slowd_period=3, slowd_matype=0)
    j = 3 * k - 2 * d
    df['KDJ_K'] = k
    df['KDJ_D'] = d
    df['KDJ_J'] = j

    # 计算波动率指标ATR指标
    df['ATR'] = talib.ATR(df['最高'].values, df['最低'].values,
                          df['收盘'].values, timeperiod=14)

    # 计算能量指标威廉指标
    df['wr'] = talib.WILLR(df['最高'].values, df['最低'].values,
                           df['收盘'].values, timeperiod=14)

    # 计算能量指标随机震荡器
    slowk, slowd = talib.STOCH(df['最高'].values, df['最低'].values, df['收盘'].values,
                               fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    df['slowk'] = slowk
    df['slowd'] = slowd

    # 计算指标
    for n in range(2, 9):
        # 过去n日ema
        df[f'EMA{n}成交量比值'] = df['成交量'] / \
            talib.MA(df['成交量'].values, timeperiod=n, matype=0)
        df[f'EMA{n}收盘比值'] = df['收盘'] / \
            talib.MA(df['收盘'].values, timeperiod=n, matype=0)
        df[f'EMA{n}开盘比值'] = df['开盘'] / \
            talib.MA(df['收盘'].values, timeperiod=n, matype=0)
        df[f'EMA{n}最高比值'] = df['最高'] / \
            talib.MA(df['收盘'].values, timeperiod=n, matype=0)
        df[f'EMA{n}最低比值'] = df['最低'] / \
            talib.MA(df['最低'].values, timeperiod=n, matype=0)

    df = df.dropna()  # 删除缺失值，避免无效数据的干扰
    for n in range(1, 8):  # 计算未来n日涨跌幅
        df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].pct_change(n).shift(-n)*100

    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码').apply(get_technical_indicators)

# 保存技术指标数据为CSV文件
grouped.to_csv(f'{name}指标.csv', index=False)
print('准备插入数据')
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
new_collection.drop()  # 清空集合中的所有文档
new_collection.insert_many(grouped.to_dict('records'))
