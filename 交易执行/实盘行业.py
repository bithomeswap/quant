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
name = ("行业")
collection = db[f"实盘{name}"]
# 获取当前日期，并通过akshare访问当日A股指数是否有数据，如果有数据则说明今日A股开盘，进行下一步的操作
# start_date = datetime.datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
day = ak.index_zh_a_hist(
    symbol="000002", start_date=start_date, period="daily")
print(day)
if not day.notna().empty:
    timestamp = datetime.datetime.strptime(
        start_date, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()
    code = ak.stock_board_industry_name_em()["板块名称"]
    try:
        latest = list(collection.find({"代码": str(code)}, {"timestamp": 1}).sort("timestamp", -1).limit(1))
        # print(latest)
        if len(latest) == 0:
            upsert_docs = True
            start_date_query = start_date
        else:
            upsert_docs = False
            latest_timestamp = latest[0]["timestamp"]
            start_date_query = datetime.datetime.fromtimestamp(
                latest_timestamp).strftime("%Y%m%d")
        # 通过 akshare 获取目标指数的日K线数据
        k_data = ak.stock_board_industry_hist_em(
            symbol=code, start_date=start_date_query, period="日k", adjust="hfq")
        k_data_true = ak.stock_board_industry_hist_em(
            symbol=code, start_date=start_date_query, period="日k", adjust="")
        try:
            k_data_true = k_data_true[["日期", "开盘"]].rename(columns={"开盘": "真实价格"})
            k_data = pd.merge(k_data, k_data_true, on="日期", how="left")
            k_data["代码"] = str(code)
            k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
            k_data["timestamp"] = k_data["日期"].apply(lambda x: float(
                datetime.datetime.strptime(x, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))
            # k_data["timestamp"] = k_data["日期"].apply(lambda x: float(
            #     datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))

            k_data = k_data.sort_values(by=["代码", "日期"])
            docs_to_update = k_data.to_dict("records")
            if upsert_docs:
                # print(f"{name}({code}) 新增数据")
                try:
                    collection.insert_many(docs_to_update)
                except Exception as e:
                    pass
            else:
                bulk_insert = []
                for doc in docs_to_update:
                    if doc["timestamp"] > latest_timestamp:
                        # 否则，加入插入列表
                        bulk_insert.append(doc)
                    if doc["timestamp"] == float(latest_timestamp):
                        try:
                            collection.update_many({"代码": doc["代码"], "日期": doc["日期"]}, {
                                "$set": doc}, upsert=True)
                        except Exception as e:
                            pass
                # 执行批量插入操作
                if bulk_insert:
                    try:
                        collection.insert_many(bulk_insert)
                    except Exception as e:
                        pass
        except Exception as e:
            print(e, f"因为{code}停牌")
        limit = 20000
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
        num = math.ceil(len(code)/100)
        df = df[(df["开盘"] >= 2) & (df["涨跌幅"] <= 0.09)].copy()
        df = df[(df[f"过去{1}日资金波动_rank"] <= 0.01)].copy()
        dfend = df.copy().groupby(["日期"], group_keys=True).apply(
            lambda x: x.nsmallest(1, f"开盘")).reset_index(drop=True)[["代码", "日期","开盘",]]
        if len(df) < 200:
            # 发布到钉钉机器人
            df["市场"] = f"实盘{name}"
            message = df[["市场","代码","日期","开盘"]].copy().to_markdown()
            print(type(message))
            webhook = "https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165"
            requests.post(webhook, json={"msgtype": "markdown", "markdown": {"title": f"{name}", "text": message}})
            
            for mes in [dfend]:
                mes["市场"] = f"实盘{name}"
                message = mes.copy().to_markdown()
                print(type(message))
                webhook = "https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165"
                requests.post(webhook, json={"msgtype": "markdown", "markdown": {
                    "title": f"{name}", "text": message}})
    except Exception as e:
        print(f"发生bug: {e}")
    buy_symbols = df["代码"].copy().drop_duplicates().tolist()  # 获取所有不重复的交易标的
    print(buy_symbols)
