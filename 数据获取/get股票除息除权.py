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

# 获取当前日期
start_date = "20170101"
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
        collection = db[f"股票除息除权{name}"]
        df = pd.DataFrame()
        df = codes[codes["代码"].str.startswith(name)][["代码", "名称"]].copy()
        # 遍历目标指数代码，获取其日K线数据
        for code in df["代码"]:
            try:
                time.sleep(3.0)
                # 通过 akshare 获取目标指数的日K线数据
                k_data = ak.stock_history_dividend_detail(
                    symbol=code, indicator="配股")
                k_data["代码"] = float(code)
                print(k_data["股权登记日"][:1])
                k_data["股权登记日"][:1] = str(k_data["股权登记日"][:1])
                k_data["除权日"][:1] = str(k_data["除权日"][:1])
                collection.insert_many(k_data.to_dict("records"))
                print(code, "已完成")
            except Exception as e:
                print(code, "未除权除息", e)
        print("任务已经完成")
    except Exception as e:
        print(e)
