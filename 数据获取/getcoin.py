# 安装币安的python库
# pip install python-binance
from binance.client import Client
from pymongo import MongoClient
import time
import pandas as pd

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要写入的数据库配置
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# 设置参数
name ="COIN"
collection = db[f"{name}"]
# 创建Binance客户端
client = Client(api_key, api_secret)
# 获取所有USDT计价的现货交易对
codes = client.get_exchange_info()["symbols"]
usdt_codes = [
    code for code in codes if code["quoteAsset"] == "USDT" and ("DOWN" not in code["symbol"]) and ("UP" not in code["symbol"])]
print(f"当前币安现货USDT有{len(usdt_codes)}个交易对")

# 遍历所有现货交易对，并获取日K线数据
for code in usdt_codes:
    symbol = code["symbol"]
    data_list = []
    # 找到该标的最新的时间戳
    latest_data = collection.find_one(
        {"代码": symbol}, {"timestamp": 1}, sort=[("timestamp", -1)])
    latest_timestamp = latest_data["timestamp"] if latest_data else 0

    totalSupply = client.get_ticker(symbol=symbol)
    print(totalSupply)

    klines = client.get_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1DAY,
        limit=1000
    )
    # 实际上实盘的时候，这里应该改成八小时
    # KLINE_INTERVAL_15MINUTE="15m"
    # KLINE_INTERVAL_8HOUR="8h"
    # KLINE_INTERVAL_1DAY ="1d"
    # 插入到集合中
    for kline in klines:
        timestamp = kline[0] / 1000
        if timestamp < latest_timestamp:  # 如果时间戳小于等于最后时间戳，直接跳过
            continue
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))
        if timestamp == latest_timestamp:
            update_data = {
                "timestamp": timestamp,
                "代码": symbol,
                "日期": date,
                "开盘": float(kline[1]),
                "最高": float(kline[2]),
                "最低": float(kline[3]),
                "收盘": float(kline[4]),
                "成交量": float(kline[5]),
                "收盘timestamp": float(kline[6]/1000),
                "成交额": float(kline[7]),
                "成交笔数": float(kline[8]),
                "主动买入成交量": float(kline[9]),
                "主动买入成交额":  float(kline[10])
            }
            filter = {"代码": symbol, "timestamp": latest_timestamp}
            collection.update_one(
                filter, {"$set": update_data})
        else:
            data_list.append({"timestamp": timestamp,
                              "代码": symbol,
                              "日期": date,
                              "开盘": float(kline[1]),
                              "最高": float(kline[2]),
                              "最低": float(kline[3]),
                              "收盘": float(kline[4]),
                              "成交量": float(kline[5]),
                              "收盘timestamp": float(kline[6]/1000),
                              "成交额": float(kline[7]),
                              "成交笔数": float(kline[8]),
                              "主动买入成交量": float(kline[9]),
                              "主动买入成交额":  float(kline[10])
                              })
    # 如果时间戳等于最新数据的时间戳，则执行更新操作，否则执行插入操作
    if len(data_list) > 0:
        collection.insert_many(data_list)

print("任务已经完成")
limit = 600000
if collection.count_documents({}) >= limit:
    oldest_data = collection.find().sort([("日期", 1)]).limit(
        collection.count_documents({})-limit)
    ids_to_delete = [data["_id"] for data in oldest_data]
    collection.delete_many({"_id": {"$in": ids_to_delete}})
print("数据清理成功")
