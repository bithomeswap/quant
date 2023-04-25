# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import talib

# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
from binance.client import Client
from pymongo import MongoClient
import time

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要写入的数据库配置
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'COIN实盘'
collection = db[f'{name}']
collection.drop()  # 清空集合中的所有文档

# 创建Binance客户端
client = Client(api_key, api_secret)

# 获取所有USDT计价的现货交易对
ticker_prices = client.get_exchange_info()['symbols']

usdt_ticker_prices = [
    ticker_price for ticker_price in ticker_prices if ticker_price['quoteAsset'] == 'USDT' and ("DOWN" not in ticker_price['symbol']) and ("UP" not in ticker_price['symbol'])]

print(f"当前币安现货有{len(ticker_prices)}个交易对")
# 遍历所有现货交易对，并获取日K线数据
for ticker_price in usdt_ticker_prices:
    symbol = ticker_price['symbol']
    klines = client.get_historical_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1DAY,
        limit=170
    )
    # 插入到集合中
    data_list = []
    for kline in klines:
        timestamp = kline[0] / 1000
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
        # 如果没有，则加入到数据列表中
        data_list.append({'timestamp': timestamp,
                          '代码': symbol,
                          '日期': date,
                          '开盘': float(kline[1]),
                          '最高': float(kline[2]),
                          '最低': float(kline[3]),
                          '收盘': float(kline[4]),
                          '成交量': float(kline[5]),
                          '收盘timestamp': float(kline[6]/1000),
                          '成交额': float(kline[7]),
                          '成交笔数': float(kline[8]),
                          '主动买入成交量': float(kline[9]),
                          '主动买入成交额':  float(kline[10])
                          })
    collection.insert_many(data_list)
    # 已经是列表对象了，不用再调用.to_dist方法
print('任务已经完成')
