# 导入需要的库
import pandas as pd
from pymongo import MongoClient
import datetime
from okx.client import Client
from okx import Account, MarketData, PublicData
# 配置API
api_key = "0282115d-3c49-4fc5-8168-326d6259f120"
secret_key = "5778C6071059A42452AE30F447DDD75F"
passphrase = "wthWTH00."

# 配置数据库
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
collection = db["OKCOIN"]

flag = "1"  # 使用1号链接
marketData = MarketData.MarketAPI(flag=flag)
# 获取日K线数据并输出到指定的数据库
data = marketData.get_tickers(instType="SPOT")

print(data)

# records = [{
#     "time": datetime.utcfromtimestamp(int(i[0])/1000).strftime("%Y-%m-%d %H:%M:%S"),
#     "open": float(i[1]),
#     "high": float(i[2]),
#     "low": float(i[3]),
#     "close": float(i[4]),
#     "volume": float(i[5])
# } for i in data]

# with MongoClient(URI) as client:
#     collection = client[DB_NAME][COLLECTION_NAME]
#     collection.insert_many(records)

# print("任务已经开始")
