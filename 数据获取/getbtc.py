# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import time
from binance.client import Client
from pymongo import MongoClient, ASCENDING

# 执行一次即可，宝塔可以设置定时执行

# 以日期为索引
# collection.create_index([('日期', ASCENDING)])

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要写入的数据库配置
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name='BTC'
collection = db[f"{name}"]


# 指定了 capped 为 True，以及最大容量的值 size（以字节为单位）
client = Client(api_key, api_secret)  # 创建Binance客户端
symbol = "BTCUSDT"
# 获取最新的15分钟K线
klines = client.get_klines(
    symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE)
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
for kline in klines:
    # 将时间戳转换为ISO格式的日期字符串
    timestamp = kline[0] / 1000
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))

    # 插入到数据库中
    if collection.count_documents({'timestamp': timestamp,
                                   '日期': date,
                                   '开盘': float(kline[1])
                                   }) == 0:
        collection.insert_one({'timestamp': timestamp,
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
                               '主动买入成交额':  float(kline[10])})

time.sleep(10)

limit = 500000
if collection.count_documents({}) >= limit:
    oldest_data = collection.find().sort([('日期', 1)]).limit(
        collection.count_documents({})-limit)
    ids_to_delete = [data['_id'] for data in oldest_data]
    collection.delete_many({'_id': {'$in': ids_to_delete}})
    # 往外读取数据的时候再更改索引吧
print('获取数据成功')
