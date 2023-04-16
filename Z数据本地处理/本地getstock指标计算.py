# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import json
import requests
import pandas as pd
import talib
import os

name = 'stock牛熊20060101_20100101'
data = pd.read_csv(f'{name}.csv')
print("数据读取成功")


def get_technical_indicators(df):  # 定义计算技术指标的函数
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

    # 计算昨日成交额
    df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
    # 成交量变成浮点数
    df['成交量'] = df['成交量'].astype(float)
    # 定义开盘幅
    df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
    # 定义开盘幅
    df['开盘开盘幅'] = (df['开盘']/df.shift(1)['开盘'] - 1)*100
    # 定义收盘幅即涨跌幅
    df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100

    df[f'EMA{121}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=121, matype=0)
    df[f'EMA{121}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=121, matype=0)
    df[f'EMA{121}最高比值'] = df['最高'] / \
        talib.MA(df['最高'].values, timeperiod=121, matype=0)
    df[f'EMA{121}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=121, matype=0)

    df[f'EMA{9}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=9, matype=0)
    df[f'EMA{4}开盘比值'] = df['开盘'] / \
        talib.MA(df['开盘'].values, timeperiod=4, matype=0)
    df[f'EMA{9}开盘动能{4}'] = df[f'EMA{4}开盘比值']/df[f'EMA{9}开盘比值']

    df[f'EMA{9}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=9, matype=0)
    df[f'EMA{4}收盘比值'] = df['收盘'] / \
        talib.MA(df['收盘'].values, timeperiod=4, matype=0)
    df[f'EMA{9}收盘动能{4}'] = df[f'EMA{4}收盘比值']/df[f'EMA{9}收盘比值']

    df[f'EMA{9}最高比值'] = df['最高'] / \
        talib.MA(df['最高'].values, timeperiod=9, matype=0)
    df[f'EMA{4}最高比值'] = df['最高'] / \
        talib.MA(df['最高'].values, timeperiod=4, matype=0)
    df[f'EMA{9}最高动能{4}'] = df[f'EMA{4}最高比值']/df[f'EMA{9}最高比值']

    df[f'EMA{9}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=9, matype=0)
    df[f'EMA{4}最低比值'] = df['最低'] / \
        talib.MA(df['最低'].values, timeperiod=4, matype=0)
    df[f'EMA{9}最低动能{4}'] = df[f'EMA{4}最低比值']/df[f'EMA{9}最低比值']

    df = df.dropna()  # 删除缺失值，避免无效数据的干扰

    df['60日最高开盘价'] = df['开盘'].rolling(60).max().shift(-60)
    df['60日最低开盘价'] = df['开盘'].rolling(60).min().shift(-60)
    df['最高开盘价日期'] = df['开盘'].rolling(60).apply(
        lambda x: x.argmax(), raw=True).shift(-60)
    df['最低开盘价日期'] = df['开盘'].rolling(60).apply(
        lambda x: x.argmin(), raw=True).shift(-60)

    for n in range(1, 9):  # 计算未来n日涨跌幅
        df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].pct_change(n).shift(-n)*100

    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码').apply(get_technical_indicators)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
parent_dir_path = os.path.dirname(os.path.dirname(dir_path))

# 保存数据到指定目录
file_path = os.path.join(parent_dir_path, f'{name}指标.csv')
grouped.to_csv(file_path, index=False)
