import pandas as pd
import akshare as ak
import pandas as pd
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
names = [("000", "001", "002", "600", "601", "603", "605")]


df = pd.merge(pd.DataFrame(list(db[f"股票('000', '001', '002', '600', '601', '603', '605')"].find())).drop('_id', axis=1), pd.DataFrame(
    list(db[f"聚宽基本面股票('000', '001', '002', '600', '601', '603', '605')"].find())).drop('_id', axis=1), on=['代码', '日期'], how='inner')
print(df)
for name in names:
    db[f"股票已拼接{name}"].drop()
    print("清理完毕准备插入数据")
    db[f"股票已拼接{name}"].insert_many(df.to_dict("records"))


dftwo = pd.merge(pd.DataFrame(list(db[f"聚宽非ST股票('000', '001', '002', '600', '601', '603', '605')"].find())).drop('_id', axis=1), pd.DataFrame(
    list(db[f"聚宽基本面股票('000', '001', '002', '600', '601', '603', '605')"].find())).drop('_id', axis=1), on=['代码', '日期'], how='inner')
print(dftwo)
for name in names:
    db[f"股票测试{name}"].drop()
    print("清理完毕准备插入数据")
    db[f"股票测试{name}"].insert_many(dftwo.to_dict("records"))
