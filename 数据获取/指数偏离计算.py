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
data = pd.DataFrame(list(collection.find({"代码": float('000002')})))
name = "ETF"
collection = db[f"{name}"]
print('任务开始')
data[['日期', '指数开盘']] = data[["日期", "开盘"]]
# 计算一年前的日期
now = datetime.datetime.now()
one_year_ago = int((now - datetime.timedelta(days=365)).timestamp())
# 根据过滤条件保留符合条件的行
data = data[data['timestamp'] >= one_year_ago]
# 输出结果
print(data)

n = 0
df = ak.fund_etf_spot_em()
# 遍历目标指数代码，获取其分钟K线数据
for code in df['代码']:
    try:
        etf = pd.DataFrame(list(collection.find(({"代码": float(f'{code}')}))))
        n += 1
        etf[['日期', f'{code}']] = etf[["日期", "开盘"]]
        if n==1:
            df = pd.merge(data[['日期', '指数开盘']], etf[['日期', f'{code}']], on='日期', how='left')
            df[f'{code}偏离'] = df[f'{code}'] / df["指数开盘"].dropna().iloc[-1]
        if n > 1:
            df = pd.merge(df, etf[['日期', f'{code}']], on='日期', how='left')
        df[f'{code}偏离'] = df[f'{code}'] / df[f'{code}'].dropna().iloc[-1]
        # df[f'{code}指数偏离'] = df[f'{code}偏离']/df['指数偏离']
        df = df.drop(f'{code}', axis=1)
        print(df)
    except Exception as e:
        print(f"发生bug: {e}")
df = df.drop(f'指数开盘', axis=1)

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f'{name}对比.csv')
df.to_csv(file_path, index=False)