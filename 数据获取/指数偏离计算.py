import os
import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
name = "指数"
collection = db[f"{name}"]
data = pd.DataFrame(list(collection.find({"代码": float('000001')})))
name = "ETF"
collection = db[f"{name}"]
dataetf = pd.DataFrame(list(collection.find({"代码": float('512010')})))
data[['日期', '指数开盘']] = data[["日期", "开盘"]]
dataetf[['日期', 'ETF开盘']] = dataetf[["日期", "开盘"]]
print(data)
df = pd.merge(data[['日期', '指数开盘']], dataetf[['日期', 'ETF开盘']], on='日期')
df['指数反向偏离'] = df["指数开盘"]/df["ETF开盘"]
print(df)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f'{name}对比.csv')
df.to_csv(file_path, index=False)
