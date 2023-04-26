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

start_date = str(20210401)
end_date = str(20230401)
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 输出的表为截止日期
name = 'STOCK'
collection = db[f"{name}"]
# collection.drop()  # 清空集合中的所有文档

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
            symbol=code, start_date=start_date, end_date=end_date, adjust="hfq")  # 历史数据后复权，确保没未来函数
        k_data_true = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date, end_date=end_date, adjust="")  # 历史数据后复权，确保没未来函数
        try:
            # k_data['真实价格'] = k_data_true['开盘价']
            k_data_true = k_data_true[['日期', '开盘']].rename(
                columns={'开盘': '真实价格'})
            # 按照时间顺序合并
            k_data = pd.merge(k_data, k_data_true, on='日期', how='left')
            k_data['代码'] = float(code)
            k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
            k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
                datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
            collection.insert_many(k_data.to_dict('records'))
        except:
            print(f"{name}({code}) 已停牌")
            continue
print('任务已经完成')


for code in stock_info_df['代码']:
    if code.startswith(('60', '000', '001')):
        latest_timestamp = collection.find(
            {"代码": code}, {"timestamp": 1}).sort("timestamp", -1).limit(1)
        if latest_timestamp.count() == 0:
            upsert_docs = True
            start_date_query = start_date
        else:
            upsert_docs = False
            latest_timestamp = list(latest_timestamp)[0]["timestamp"]
            start_date_query = datetime.datetime.fromtimestamp(
                latest_timestamp).strftime('%Y%m%d')

        k_data = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date_query, end_date=end_date, adjust="hfq")
        k_data_true = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date_query, end_date=end_date, adjust="")
        k_data_true = k_data_true[['日期', '开盘']].rename(columns={'开盘': '真实价格'})
        k_data = pd.merge(k_data, k_data_true, on='日期', how='left')
        k_data['代码'] = float(code)
        k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
        k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
            datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
        k_data = k_data.sort_values(by=["代码", "日期"])
        docs_to_update = k_data.to_dict('records')
        if upsert_docs:
            try:
                collection.insert_many(docs_to_update)
            except Exception as e:
                pass
        else:
            for doc in docs_to_update:
                if doc["timestamp"] > latest_timestamp:
                    try:
                        collection.update_many({"代码": doc["代码"], "日期": doc["日期"]}, {
                                               "$set": doc}, upsert=True)
                    except Exception as e:
                        pass
print('任务已经完成')
# time.sleep(60)
# limit = 400000
# if collection.count_documents({}) >= limit:
#     oldest_data = collection.find().sort([('日期', 1)]).limit(
#         collection.count_documents({})-limit)
#     ids_to_delete = [data['_id'] for data in oldest_data]
#     collection.delete_many({'_id': {'$in': ids_to_delete}})
#     # 往外读取数据的时候再更改索引吧
# print('数据清理成功')
