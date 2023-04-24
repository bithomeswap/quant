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

start_date = str(20200401)
end_date = str(20230401)
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 输出的表为截止日期
name = 'STOCK'
collection = db[f"{name}"]
collection.drop()  # 清空集合中的所有文档

# 从akshare获取A股主板股票的代码和名称
stock_info_df = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('ST')]
# 过滤掉退市股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('退')]
# 迭代每只股票，获取每天的前复权日k数据
for code in stock_info_df['代码']:
    if code.startswith(('60', '000', '001')):
        k_data = ak.stock_zh_a_hist(
            # symbol=code, start_date=start_date, end_date=end_date, adjust="hfq")
            # 历史数据后复权，确保没负数
            symbol=code, start_date=start_date, end_date=end_date, adjust="qfq")
            # 近期数据前复权，确保真数据
        try:
            k_data['代码'] = float(code)
            k_data['timestamp'] = k_data['日期'].apply(lambda x: float(datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
            collection.insert_many(k_data.to_dict('records'))
        except:
            print(f"{name}({code}) 已停牌")
            continue
print('任务已经完成')

klines = ak.index_zh_a_hist(
    symbol="000001", period="daily", start_date=start_date, end_date=end_date)
# collection.insert_many(klines.to_dict('records'))
for kline in klines.to_dict("records"):
    if kline["开盘"] == 0:
        continue
    kline["代码"] = float(000000.0)
    kline["timestamp"] = float(datetime.datetime.strptime(
        kline["日期"], "%Y-%m-%d").timestamp())
    kline["成交量"] = float(kline["成交量"])
    query = {"日期": kline["日期"]}
    result = collection.find_one(query)
if result:
    print("数据重复")
else:
    collection.insert_one(kline)

# query = {'代码': {'$exists': False}}
# result = collection.delete_many(query)
# print(f'已删除 {result.deleted_count} 条记录')

# # time.sleep(3600)
# limit = 2000000
# if collection.count_documents({}) >= limit:
#     oldest_data = collection.find().sort([('日期', 1)]).limit(
#         collection.count_documents({})-limit)
#     ids_to_delete = [data['_id'] for data in oldest_data]
#     collection.delete_many({'_id': {'$in': ids_to_delete}})
#     # 往外读取数据的时候再更改索引吧
# print('数据清理成功')
