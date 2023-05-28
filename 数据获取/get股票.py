import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
names = [("000", "001", "002", "600", "601", "603", "605")]

# # 获取当前日期
# start_date = "20170101"
# current_date = datetime.datetime.now()
# end_date = current_date.strftime("%Y%m%d")
# 从akshare获取A股主板股票的代码和名称
codes = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
codes = codes[~codes["名称"].str.contains("ST")]
# 过滤掉退市股票
codes = codes[~codes["名称"].str.contains("退")]
for name in names:
    try:
        collection = db[f"股票{name}"]
        df = pd.DataFrame()
        df = codes[codes["代码"].str.startswith(name)][["代码", "名称"]].copy()
        # 遍历目标指数代码，获取其日K线数据
        for code in df["代码"]:
            # print(code)
            latest = list(collection.find({"代码": float(code)}, {
                          "timestamp": 1}).sort("timestamp", -1).limit(1))
            # print(latest)
            if len(latest) == 0:
                upsert_docs = True
                # start_date_query = start_date
            else:
                upsert_docs = False
                latest_timestamp = latest[0]["timestamp"]
                start_date_query = datetime.datetime.fromtimestamp(
                    latest_timestamp).strftime("%Y%m%d")

            # # 通过 akshare 获取目标指数的日K线数据
            # k_data = ak.stock_zh_a_hist(
            #     symbol=code, start_date=start_date_query, end_date=end_date, adjust="hfq")
            # k_data_true = ak.stock_zh_a_hist(
            #     symbol=code, start_date=start_date_query, end_date=end_date, adjust="")

            # 通过 akshare 获取目标指数的日K线数据
            k_data = ak.stock_zh_a_hist(
                symbol=code, adjust="hfq")
            k_data_true = ak.stock_zh_a_hist(
                symbol=code, adjust="")
            try:
                k_value = ak.stock_zh_valuation_baidu(
                    symbol=code,  indicator="总市值", period="全部")
                k_value.rename(
                    columns={"date": "日期", "value": "总市值"}, inplace=True)
            except Exception as e:
                print(e, f"{code}百度基本面数据拼接错误")          
            try:
                k_data_true = k_data_true[["日期", "开盘"]].rename(columns={"开盘": "真实价格"})
                k_data = pd.merge(k_data, k_data_true, on="日期", how="left")
                k_data = pd.merge(k_data, k_value, on="日期", how="left")

                k_data["代码"] = float(code)
                k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
                k_data["timestamp"] = k_data["日期"].apply(lambda x: float(datetime.datetime.strptime(x, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))
                # k_data["timestamp"] = k_data["日期"].apply(lambda x: float(datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))

                k_data = k_data.sort_values(by=["代码", "日期"])
                docs_to_update = k_data.to_dict("records")
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
                print(e, f"因为{code}停牌")
        print("任务已经完成")
        # limit = 600000
        # if collection.count_documents({}) >= limit:
        #     oldest_data = collection.find().sort([("日期", 1)]).limit(
        #         collection.count_documents({})-limit)
        #     ids_to_delete = [data["_id"] for data in oldest_data]
        #     collection.delete_many({"_id": {"$in": ids_to_delete}})
        # print("数据清理成功")
    except Exception as e:
        print(e)
