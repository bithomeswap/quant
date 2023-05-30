import datetime
import pandas as pd
import os
from pymongo import MongoClient


def technology(df):  # 定义计算技术指标的函数
    try:
        # df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        df.drop(df[(df["最高"] < 0) | (df["最低"] < 0)].index,
                inplace=True)  # 删除价格为负的异常数据
        df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
        df["开盘收盘幅"] = df["开盘"]/df["收盘"].copy().shift(1) - 1
        df["涨跌幅"] = df["收盘"]/df["收盘"].copy().shift(1) - 1
        df["昨日成交额"] = df["成交额"].copy().shift(1)
        df["昨日振幅"] = (df["最高"].copy().shift(1)-df["最低"].copy().shift(1))/df["开盘"].copy().shift(1)
        
        df["昨日涨跌"] = df["涨跌幅"].copy().shift(1)+1
        df["昨日资金贡献"] = df["昨日涨跌"] / df["昨日成交额"]
        df["昨日资金波动"] = df["昨日振幅"] / df["昨日成交额"]
        if ("股票" in name)| ("COIN" in name):
            df["昨日总市值"] = df["总市值"].copy().shift(1)
            df["昨日市值获利"] = df["昨日涨跌"] / df["昨日总市值"]
            df["昨日市值资金贡献"] = df["昨日资金贡献"]/df["昨日总市值"]
            df["昨日市值资金波动"] = df["昨日资金波动"]/df["昨日总市值"]
            df["昨日市值成交"] = df["昨日成交额"]*df["昨日总市值"]
        if ("分钟" in name) | ("指数" in name) | ("行业" in name):
            for n in range(1, 10):
                df[f"过去{n}日总涨跌"] = df["开盘"]/(df["开盘"].copy().shift(n))
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def rank(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if ("未来函数" not in str(column)):
            df = pd.concat([df, (df[str(column)].rank(
                method="max", ascending=False) / len(df)).rename(f"{str(column)}_rank")], axis=1)
    return df


def tradelist(name):
    collection = db[f"{name}"]
    # # 获取数据并转换为DataFrame格式
    if "股票" in name:  # 数据截取
        watchtime = 2017
        df = pd.DataFrame(list(collection.find(
            {"日期": {"$gt": datetime.datetime(watchtime, 1, 1).strftime("%Y-%m-%d")}})))
        # 按照“代码”列进行分组并计算技术指标
        df = df.groupby(["代码"], group_keys=False).apply(technology)
        # 分组并计算指标排名
        df = df.groupby(["日期"], group_keys=False).apply(rank)
        # 连接MongoDB数据库并创建新集合
        new_collection = db[f"{name}{watchtime}指标"]
    if "COIN" in name:  # 数据截取
        watchtime = 2020
        df = pd.DataFrame(list(collection.find(
            {"日期": {"$gt": datetime.datetime(watchtime, 1, 1).strftime("%Y-%m-%d")}})))
        dfbase = pd.DataFrame(list(db[f"{name}基本面"].find()))
        df = pd.merge(df, dfbase[["代码", "发行量"]], on="代码")
        df["总市值"]=df["开盘"]*df["发行量"]
        # 按照“代码”列进行分组并计算技术指标
        df = df.groupby(["代码"], group_keys=False).apply(technology)
        # 分组并计算指标排名
        df = df.groupby(["日期"], group_keys=False).apply(rank)
        # 连接MongoDB数据库并创建新集合
        new_collection = db[f"{name}{watchtime}指标"]
    else:
        df = pd.DataFrame(list(collection.find()))
        # 按照“代码”列进行分组并计算技术指标
        df = df.groupby(["代码"], group_keys=False).apply(technology)
        # 分组并计算指标排名
        df = df.groupby(["日期"], group_keys=False).apply(rank)
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
    new_collection.insert_many(df.to_dict("records"))
    print(f"{name}数据插入结束")


# 连接MongoDB数据库
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# 获取当前数据库中的所有集合名称
names = list(db.list_collection_names())
print(names)
for name in names:
    if ("指标" not in name) & ("实盘" not in name) & ("order" not in name) & ("js" not in name):
        # if ("分钟" not in name) & ("股票" in name):
        # if ("分钟" in name):
        # if ("行业" in name) | ("指数" in name):
        if ("COIN" in name):
            print(f"当前计算{name}")
            try:
                tradelist(name)
            except Exception as e:
                print(f"tradelist发生bug: {e}")
