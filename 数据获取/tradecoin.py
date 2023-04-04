# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import numpy as np
import time
from binance.client import Client
from pymongo import MongoClient

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要读取的数据库配置
client_read = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db_read = client_read['wth000']
collection_read = db_read['BTC']

# 需要写入的数据库配置
client_write = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db_write = client_write['wth000']
name = 'BTC'
collection_write = db_write[f'{name}order']

# 创建Binance客户端
client = Client(api_key, api_secret)

while True:
    print('获取数据成功')
    # 获取最新的15分钟K线
    klines = client.get_klines(
        symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_1MINUTE)
    # KLINE_INTERVAL_1MINUTE = '1m'
    # KLINE_INTERVAL_3MINUTE = '3m'
    # KLINE_INTERVAL_5MINUTE = '5m'
    # KLINE_INTERVAL_15MINUTE = '15m'
    # KLINE_INTERVAL_30MINUTE = '30m'
    # KLINE_INTERVAL_1HOUR = '1h'
    # KLINE_INTERVAL_2HOUR = '2h'
    # KLINE_INTERVAL_4HOUR = '4h'
    # KLINE_INTERVAL_6HOUR = '6h'
    # KLINE_INTERVAL_8HOUR = '8h'
    # KLINE_INTERVAL_12HOUR = '12h'
    # KLINE_INTERVAL_1DAY = '1d'
    # KLINE_INTERVAL_3DAY = '3d'
    # KLINE_INTERVAL_1WEEK = '1w'
    # KLINE_INTERVAL_1MONTH = '1M'
    # 计算指标
    close = np.array([float(kline[4]) for kline in klines])
    ma5 = np.mean(close[-5:])
    ma10 = np.mean(close[-10:])
    ma20 = np.mean(close[-20:])
    rsi14 = 100 - 100 / \
        (1 + np.mean(np.maximum(0, close[-14:] -
         np.roll(close[-14:], 1))))   # RSI14
    rsi6 = 100 - 100 / \
        (1 + np.mean(np.maximum(0, close[-6:] -
         np.roll(close[-6:], 1))))     # RSI6

    # 根据指标生成交易指令
    if ma5 > ma10 and ma10 > ma20 and rsi14 < 30 and rsi6 < 20:
        order = client.create_order(
            symbol='BTCUSDT',
            side='BUY',
            type='MARKET',
            quantity=0.01)
        collection_write.insert_one(order)
    elif ma5 < ma10 and ma10 < ma20 and rsi14 > 70 and rsi6 > 80:
        order = client.create_order(
            symbol='BTCUSDT',
            side='SELL',
            type='MARKET',
            quantity=0.01)
        collection_write.insert_one(order)

    # 休眠1分钟
    time.sleep(60)
