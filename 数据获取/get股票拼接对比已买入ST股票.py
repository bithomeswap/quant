import math
import choose
import pandas as pd
import os
import datetime
from pymongo import MongoClient
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
collection=db[f"策略买入ST股票('000', '001', '002', '600', '601', '603', '605')"]
collection.drop()
moneyused = 0.9  # 设置资金利用率
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
path = os.path.join(
    dir_path, f"股票('000', '001', '002', '600', '601', '603', '605')指标周期30交易细节.csv")
df = pd.read_csv(path)
df = df[["代码", "日期"]]
for idx, row in df.iterrows():
    code = row['代码']
    date = row['日期']
    # dfst = pd.DataFrame(list(
    #     db[f"聚宽ST股票('000', '001', '002', '600', '601', '603', '605')"].find({"代码": code, "日期": date})))
    dfst = pd.DataFrame(list(
        db[f"股票除息除权('000', '001', '002', '600', '601', '603', '605')"].find({"代码": code, "除权日": date})))

    print(code, date, dfst)
    if not dfst.empty:
        collection.insert_many(dfst.to_dict("records"))
