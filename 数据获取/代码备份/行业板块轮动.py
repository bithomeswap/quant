
import os
import pandas as pd
import datetime
from pymongo import MongoClient

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]

data = pd.DataFrame(list(db[f"指数指标"].find({"代码": float("000002")})))
# 计算一年前的日期
now = datetime.datetime.now()
ago = int((now - datetime.timedelta(days=2000)).timestamp())
data = data[data["timestamp"] >= ago]

name = "行业"
df = pd.DataFrame(list(db[f"{name}指标"].find()))
print(f"{name}数据读取成功")

m = 5  # 以过去m日涨跌为比较基准
n = 2  # 以过去n日涨跌为确认基准
data[["日期", f"指数过去{m}日总涨跌", f"指数过去{n}日总涨跌"]] = data[["日期", f"过去{m}日总涨跌", f"过去{n}日总涨跌"]]

def technology(df):  # 定义计算技术指标的函数
    try:
        # 删除最高价和最低价为负值的数据
        df = pd.merge(df, data[["日期", f"指数过去{m}日总涨跌", f"指数过去{n}日总涨跌"]], on="日期", how="left")
        df.sort_values(by="日期")    # 以日期列为索引,避免计算错误
        for a in range(1, 16):
            df[f"{a}日后总涨跌幅（未来函数）"] = (df["收盘"].copy().shift(-a) / df["收盘"]) - 1
        df[f"指数{m}日偏离"] = df[f"过去{m}日总涨跌"]-df[f"指数过去{m}日总涨跌"]
        df[f"指数{n}日偏离"] = df[f"过去{n}日总涨跌"]-df[f"指数过去{n}日总涨跌"]
        # 判断金叉和死叉的条件，昨天小今天大是金叉，昨天大今天小是死叉
        df.loc[(df[f"指数{m}日偏离"] >= 0) & (
            df[f"指数{m}日偏离"].shift(1) < 0), f"过去{m}日涨跌{0}金叉"] = 1
        df.loc[(df[f"指数{m}日偏离"] <= 0) & (
            df[f"指数{m}日偏离"].shift(1) > 0), f"过去{m}日涨跌{0}死叉"] = 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


# 按照“代码”列进行分组并计算技术指标
df = df.groupby(["代码"], group_keys=False).apply(technology)
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f"{name}偏离指标.csv")
df.to_csv(file_path, index=False)
