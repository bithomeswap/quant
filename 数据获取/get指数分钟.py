import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]

# 设置参数
# name ='指数'
name ='指数分钟'
# name ='COIN'
# name ='COIN分钟'
# name = 'ETF'

collection = db[f"{name}"]
# 获取当前日期
current_date = datetime.datetime.now()
# 读取180天内的数据，这里面还得排除掉节假日,初始数据建议220,实际更新的时候更新15天就行
date_ago = current_date - datetime.timedelta(days=1000)
start_date = date_ago.strftime('%Y%m%d')  # 要求格式"19700101"
end_date = current_date.strftime('%Y%m%d')

# 获取A股指数代码列表
df =                   ['000002',
                    # 中证行业指数
                    '000986', '000987', '000988', '000989', '000990', '000991', '000992', '000993', '000994', '000995',
                    # 深证行业指数
                    '399613', '399614', '399615', '399616', '399617', '399618', '399619', '399620', '399621', '399622',
                    # 上证等权指数
                    '000070', '000071', '000072', '000073', '000074', '000075', '000076', '000077', '000078', '000079',
                    ]
# 遍历目标指数代码，获取其分钟K线数据
for code in df:
    # print(code)
    latest = list(collection.find({"代码": float(code)}, {
                  "timestamp": 1}).sort("timestamp", -1).limit(1))
    # print(latest)
    if len(latest) == 0:
        upsert_docs = True
        start_date_query = start_date
    else:
        upsert_docs = False
        latest_timestamp = latest[0]["timestamp"]
        start_date_query = datetime.datetime.fromtimestamp(
            latest_timestamp).strftime('%Y%m%d')

    # 通过 akshare 获取目标指数的分钟K线数据
    k_data = ak.index_zh_a_hist_min_em(symbol=code, period="1")
    k_data = k_data[k_data["开盘"] != 0]

    try:
        k_data['代码'] = float(code)
        k_data["日期"] = k_data["时间"]
        k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))

        k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
            datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.timezone('Asia/Shanghai')).timestamp()))
        # k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
        #     datetime.datetime.strptime(x, '%Y-%m-%d').replace(tzinfo=pytz.timezone('Asia/Shanghai')).timestamp()))

        k_data = k_data.sort_values(by=["代码", "日期"])
        docs_to_update = k_data.to_dict('records')
        if upsert_docs:
            # print(f"{name}({code}) 新增数据")
            try:
                collection.insert_many(docs_to_update)
            except Exception as e:
                pass
        else:
            bulk_insert = []
            for doc in docs_to_update:
                if doc["timestamp"] > latest_timestamp:
                    # 否则，加入插入列表
                    bulk_insert.append(doc)
                if doc["timestamp"] == float(latest_timestamp):
                    try:
                        collection.update_many({"代码": doc["代码"], "日期": doc["日期"]}, {
                            "$set": doc}, upsert=True)
                    except Exception as e:
                        pass
            # 执行批量插入操作
            if bulk_insert:
                try:
                    collection.insert_many(bulk_insert)
                except Exception as e:
                    pass
    except Exception as e:
        print(e, f'因为{code}停牌')
print('任务已经完成')
# time.sleep(60)
limit = 600000
if collection.count_documents({}) >= limit:
    oldest_data = collection.find().sort([('日期', 1)]).limit(
        collection.count_documents({})-limit)
    ids_to_delete = [data['_id'] for data in oldest_data]
    collection.delete_many({'_id': {'$in': ids_to_delete}})
    # 往外读取数据的时候再更改索引吧
print('数据清理成功')
