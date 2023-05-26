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

# print(data)
# from okx.client import Client
# from pymongo import MongoClient

# def main():
#     # 创建 OKEx 客户端
#     client = Client(api_key, secret_key, passphrase, True)

#     # 获取现货交易对列表
#     all_instruments = client.get_instruments()
#     usdt_pairs = [instrument for instrument in all_instruments if instrument.currency_quote == 'USDT']

#     # 获取日K线数据并批量插入到MongoDB
#     for pair in usdt_pairs:
#         print(f'开始获取{pair.instrument_id}的日K线数据')
#         kline = client.get_kline(pair.instrument_id, 86400, 200)
        
#         # 处理数据
#         data = []
#         for item in kline:
#             data.append({
#                 'time': item[0],
#                 'open': item[1],
#                 'high': item[2],
#                 'low': item[3],
#                 'close': item[4],

#                 # 数据中有些交易对没有成交量，因此需要做容错处理
#                 'volume': item[5] if len(item) > 5 else 0,
#             })

#         # 批量插入数据到MongoDB
#         if len(data) > 0:
#             collection.insert_many(data)
#             print(f'{pair.instrument_id}数据写入成功')
#         else:
#             print(f'{pair.instrument_id}没有可写入的数据')

# if __name__ == '__main__':
#     print('任务已经开始')
#     main()
