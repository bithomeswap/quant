import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import pytz
import requests
import pandas as pd
import time
import math


def technology(df):  # 定义计算技术指标的函数
    try:
        # df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
        df["涨跌幅"] = df["收盘"]/df["收盘"].copy().shift(1) - 1
        df["振幅"] = (df["最高"].copy()-df["最低"].copy())/df["开盘"].copy()
        df["资金波动"] = df["振幅"] / df["成交额"]
        for n in range(1, 5):
            df[f"过去{n}日资金波动"] = df["资金波动"].shift(n)
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


client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
name = ("000", "001", "002", "600", "601", "603", "605")
collection = db[f"实盘{name}"]
# 获取当前日期，并通过akshare访问当日A股指数是否有数据，如果有数据则说明今日A股开盘，进行下一步的操作
start_date = datetime.datetime.now().strftime("%Y-%m-%d")
day = ak.index_zh_a_hist(
    symbol="000002", start_date=start_date, period="daily")
print(day)
if not day.notna().empty:
    timestamp = datetime.datetime.strptime(
        start_date, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()
    k_data = ak.stock_zh_a_spot_em()    # 从akshare获取A股主板股票实时数据
    k_data = k_data[~k_data["名称"].str.contains("ST")]    # 过滤掉ST股票
    k_data = k_data[~k_data["名称"].str.contains("退")]    # 过滤掉退市股票
    try:
        k_data = k_data[k_data["代码"].str.startswith(name)]
        k_data["开盘"] = k_data["今开"]
        k_data["收盘"] = k_data["最新价"]
        k_data["代码"] = k_data["代码"].apply(lambda x: float(x))
        k_data["总市值"] = k_data["成交额"]/(k_data["换手率"]/100)
        latest = list(collection.find({"timestamp": timestamp}, {
                      "timestamp": 1}).sort("timestamp", -1).limit(1))
        if len(latest) == 0:
            upsert_docs = True
            start_date_query = start_date
            print(latest)
        else:
            upsert_docs = False
            latest_timestamp = latest[0]["timestamp"]
            start_date_query = datetime.datetime.fromtimestamp(
                latest_timestamp).strftime("%Y-%m-%d")
        try:
            k_data["timestamp"] = timestamp
            k_data["日期"] = start_date
            k_data["代码"] = k_data["代码"].apply(lambda x: float(x))
            k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
            k_data = k_data.to_dict("records")
            if upsert_docs:
                collection.insert_many(k_data)
            else:
                bulk_insert = []
                for doc in k_data:
                    print(doc["代码"], "数据更新")
                    if doc["timestamp"] > latest_timestamp:
                        # 否则，加入插入列表
                        bulk_insert.append(doc)
                    if doc["timestamp"] == float(latest_timestamp):
                        collection.update_many({"代码": doc["代码"], "timestamp": float(timestamp)}, {
                                               "$set": doc}, upsert=True)
                # 执行批量插入操作
                if bulk_insert:
                    collection.insert_many(bulk_insert)
            print("任务已经完成")
        except Exception as e:
            print(e)
        limit = 50000
        if collection.count_documents({}) >= limit:
            oldest_data = collection.find().sort([("日期", 1)]).limit(
                collection.count_documents({})-limit)
            ids_to_delete = [data["_id"] for data in oldest_data]
            collection.delete_many({"_id": {"$in": ids_to_delete}})
        print("数据清理成功")
    except Exception as e:
        print(e)
    time.sleep(1)
    # 获取数据并转换为DataFrame格式
    df = pd.DataFrame(list(collection.find()))
    print(f"{name}数据读取成功")
    # 按照“代码”列进行分组并计算技术指标
    df = df.groupby(["代码"], group_keys=False).apply(technology)
    # 分组并计算指标排名
    df = df.groupby(["日期"], group_keys=False).apply(rank)
    try:
        df.sort_values(by="日期")    # 以日期列为索引,避免计算错误
        # 获取最后一天的数据
        last_day = df.iloc[-1]["日期"]
        # 计算总共统计的股票数量
        df = df[df[f"日期"] == last_day].copy()
        code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
        value = math.floor(math.log10(len(code)))  # 整数位数
        num = math.ceil(len(code)/(10**value))  # 持仓数量
        df = df[(df["开盘"] >= 4)].copy()  # 过滤低价股
        df = df[(df["开盘收盘幅"] <= 0.08)].copy()  # 过滤可能产生大回撤的股票
        df = df[(df["昨日资金波动_rank"] <= 0.01)].copy()
        df = df.groupby(["日期"], group_keys=True).apply(
            lambda x: x.nsmallest(num, "昨日总市值")).reset_index(drop=True)
        print(df)
        if len(df) < 200:
            # 发布到钉钉机器人
            df["市场"] = f"实盘{name}"
            message = df[["市场", "代码", "日期", "开盘"]].copy().to_markdown()
            print(type(message))
            webhook = "https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165"
            requests.post(webhook, json={"msgtype": "markdown", "markdown": {
                "title": f"{name}", "text": message}})
    except Exception as e:
        print(f"发生bug: {e}")
    buy_symbols = df["代码"].copy().drop_duplicates().tolist()  # 获取所有不重复的交易标的
    print(buy_symbols)
