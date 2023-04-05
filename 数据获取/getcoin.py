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
name = 'COIN'
collection = db[f'{name}']
collection.drop()  # 清空集合中的所有文档

# 创建Binance客户端
client = Client(api_key, api_secret)

# 获取所有USDT计价的现货交易对
ticker_prices = client.get_exchange_info()['symbols']
usdt_ticker_prices = [
    ticker_price for ticker_price in ticker_prices if ticker_price['quoteAsset'] == 'USDT']
print(f"当前币安现货有{len(ticker_prices)}个交易对")
# 遍历所有现货交易对，并获取日K线数据
for ticker_price in usdt_ticker_prices:
    symbol = ticker_price['symbol']
    klines = client.get_historical_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1DAY,
        limit=2000
    )
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
# time.sleep(3600)
# limit = 400000
# if collection.count_documents({}) >= limit:
#     oldest_data = collection.find().sort([('日期', 1)]).limit(
#         collection.count_documents({})-limit)
#     ids_to_delete = [data['_id'] for data in oldest_data]
#     collection.delete_many({'_id': {'$in': ids_to_delete}})
#     # 往外读取数据的时候再更改索引吧
# print('数据清理成功')
