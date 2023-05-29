import matplotlib.pyplot as plt
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
collection = db[f"{name}指标"]
data = pd.DataFrame(list(collection.find({"代码": float("000002")})))
data[["日期", f"指数过去{5}日总涨跌", f"指数过去{2}日总涨跌"]
     ] = data[["日期", f"过去{5}日总涨跌", f"过去{2}日总涨跌"]]

name = "行业"
df = pd.DataFrame({"代码": ak.stock_board_industry_name_em()["板块名称"]})

collection = db[f"{name}指标"]
# 计算一年前的日期
now = datetime.datetime.now()
ago = int((now - datetime.timedelta(days=1000)).timestamp())
data = data[data["timestamp"] >= ago]
print(data)
n = 0
# 遍历目标指数代码，获取其分钟K线数据
for code in df["代码"]:
    print(type(code))
    try:
        n += 1
        print(n)
        if ("行业" in name.lower()):
            etf = pd.DataFrame(list(collection.find(({"代码": str(code)}))))
            etf[["日期", f"{code}过去{5}日总涨跌", f"{code}过去{2}日总涨跌"]
                ] = etf[["日期", f"过去{5}日总涨跌", f"过去{2}日总涨跌"]]
        if n == 1:
            df = pd.merge(data[["日期", f"指数过去{5}日总涨跌", f"指数过去{2}日总涨跌"]],
                          etf[["日期", f"{code}过去{5}日总涨跌", f"{code}过去{2}日总涨跌"]], on="日期", how="left")
        if n > 1:
            df = pd.merge(
                df, etf[["日期", f"{code}过去{5}日总涨跌", f"{code}过去{2}日总涨跌"]], on="日期", how="left")
        df[f"{code}{5}指数偏离"] = df[f"{code}过去{5}日总涨跌"]-df[f"指数过去{5}日总涨跌"]
        df[f"{code}{2}指数偏离"] = df[f"{code}过去{2}日总涨跌"]-df[f"指数过去{2}日总涨跌"]
        print(df.loc[:0])
    except Exception as e:
        print(f"发生bug: {e}")
    if n == 300:
        break
# df = df.dropna(axis=1)  # 删除所有含有空值的列
df = df.fillna(0)

# 绘图
# # 设置中文字体和短横线符号
# plt.rcParams["font.family"] = ["Microsoft YaHei"]
# plt.rcParams["axes.unicode_minus"] = False
# plt.figure(figsize=(16, 8))
# plt.plot(df.set_index("日期"))
# plt.legend(df.columns.drop("日期"))
# plt.xlabel("日期")
# plt.ylabel("指数偏离度")
# plt.title(f"{n}种{name}指数对比")
# # plt.ylim(0)
# plt.show()

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f"{name}强弱对比.csv")
df.to_csv(file_path, index=False)
