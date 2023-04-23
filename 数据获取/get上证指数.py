import akshare as ak
from pymongo import MongoClient
import datetime

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]

name = "上证指数"
collection = db[f"{name}"]

klines = ak.index_zh_a_hist_min_em(symbol="000001", period="1")

for kline in klines.to_dict("records"):
    if kline["开盘"] == 0:
        continue
    kline["timestamp"] = float(datetime.datetime.strptime(
        kline["时间"], "%Y-%m-%d %H:%M:%S").timestamp())
    kline["日期"] = kline["时间"]
    kline["成交量"] = float(kline["成交量"])
    query = {"日期": kline["日期"]}
    result = collection.find_one(query)

    if result:
        print("数据重复")
    else:
        collection.insert_one(kline)

# 删除超出限制的数据
limit = 500000
if collection.count_documents({}) >= limit:
    oldest_data = collection.find().sort([("日期", 1)]).limit(
        collection.count_documents({}) - limit
    )
    ids_to_delete = [data["_id"] for data in oldest_data]
    collection.delete_many({"_id": {"$in": ids_to_delete}})
print("获取数据成功")
# time.sleep(3600)
limit = 20000
if collection.count_documents({}) >= limit:
    oldest_data = collection.find().sort([('日期', 1)]).limit(
        collection.count_documents({})-limit)
    ids_to_delete = [data['_id'] for data in oldest_data]
    collection.delete_many({'_id': {'$in': ids_to_delete}})
    # 往外读取数据的时候再更改索引吧
print('数据清理成功')
