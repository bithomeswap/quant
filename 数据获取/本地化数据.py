from pymongo import MongoClient
import pandas as pd
import os
import datetime
# MongoDB数据库配置
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# 设置参数
names = [("000", "001", "002", "600", "601", "603", "605")]
for name in names:
    collection = db[f"聚宽股票{name}"]
    # 读取MongoDB中的数据
    df = pd.DataFrame(list(collection.find()))
    print("数据读取成功")
    # if "股票" in name:  # 数据截取
    #     watchtime = 2019
    #     start_date = datetime.datetime(
    #         watchtime, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
    #     df = df[(df["日期"] >= start_date)]
    # 获取当前.py文件的绝对路径
    file_path = os.path.abspath(__file__)
    # 获取当前.py文件所在目录的路径
    dir_path = os.path.dirname(file_path)
    # 获取当前.py文件所在目录的上两级目录的路径
    dir_path = os.path.dirname(os.path.dirname(dir_path))
    file_path = os.path.join(dir_path, f"{name}.csv")
    df.to_csv(file_path, index=False)
