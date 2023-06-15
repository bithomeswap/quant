import datetime
import pandas as pd
import os
from pymongo import MongoClient
import choose


def technology(df):  # 定义计算技术指标的函数
    try:
        df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
        df["涨跌幅"] = df["收盘"]/df["收盘"].copy().shift(1) - 1
        df["振幅"] = (df["最高"].copy()-df["最低"].copy())/df["开盘"].copy()
        df["资金波动"] = df["振幅"] / df["成交额"]
        for n in range(1, 5):
            df[f"过去{n}日资金波动"] = df["资金波动"].shift(n)
            df[f"过去{n}日总涨跌"] = df["开盘"]/(df["开盘"].copy().shift(n))
        if "股票已拼接" in name:
            df["营收市值比"] = df["营收"]/df["总市值"]
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def tradelist(name):
    collection = db[f"{name}"]
    # # 获取数据并转换为DataFrame格式
    if ("股票" in name)and ("可转债" not in name):  # 数据截取
        watchtime = 2017
        df = pd.DataFrame(list(collection.find({"日期": {"$gt": datetime.datetime(
            watchtime, 1, 1).strftime("%Y-%m-%d")}}))).drop('_id', axis=1)
        # 按照“代码”列进行分组并计算技术指标
        df = df.groupby("代码", group_keys=False).apply(technology)
        df = df.groupby("日期", group_keys=False).apply(choose.rank)
        # 连接MongoDB数据库并创建新集合
        new_collection = db[f"{name}{watchtime}指标"]
    else:  # 数据截取
        df = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
        # 按照“代码”列进行分组并计算技术指标
        df = df.groupby("代码", group_keys=False).apply(technology)
        df = df.groupby("日期", group_keys=False).apply(choose.rank)
        # 连接MongoDB数据库并创建新集合
        new_collection = db[f"{name}指标"]
    new_collection.drop()  # 清空集合中的所有文档
    # 获取当前.py文件的绝对路径
    file_path = os.path.abspath(__file__)
    # 获取当前.py文件所在目录的路径
    dir_path = os.path.dirname(file_path)
    # 获取当前.py文件所在目录的上两级目录的路径
    dir_path = os.path.dirname(os.path.dirname(dir_path))
    # 保存数据到指定目录
    file_path = os.path.join(dir_path, f"{name}指标.csv")
    df.to_csv(file_path, index=False)
    print(f"{name}准备插入数据")
    # new_collection.insert_many(df.to_dict("records"))
    # print(f"{name}数据插入结束")


# 连接MongoDB数据库
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# 获取当前数据库中的所有集合名称
names = list(db.list_collection_names())
print(names)
for name in names:
    if ("指标" not in name) & ("实盘" not in name) & ("聚宽" not in name) & ("测试" not in name) & ("ST" not in name) & ("order" not in name) & ("js" not in name):
        # if ("分钟" not in name):
        # if ("分钟" in name):
        # if ("行业" in name) | ("指数" in name):
        # if ("ETF" in name)|("COIN" in name)|("股票30分钟" in name)|("股票" in name):
        if ("可转债" in name):
            print(f"当前计算{name}")
            try:
                tradelist(name)
            except Exception as e:
                print(f"tradelist发生bug: {e}")
