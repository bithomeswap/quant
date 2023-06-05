import math
import choose
import pandas as pd
import os
import datetime
from pymongo import MongoClient
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
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
df = pd.merge(df, pd.DataFrame(list(
    db[f"聚宽非ST股票('000', '001', '002', '600', '601', '603', '605')"].find())), on=['代码', '日期'], how='inner')
df.to_csv("策略买入的ST股票.csv")
print(df)
