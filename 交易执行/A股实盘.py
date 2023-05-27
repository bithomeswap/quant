import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import pytz
import requests
import pandas as pd
import time
import math


def choose(choosename, name, df):
    if choosename == "交易":
        code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
        rank = math.ceil(len(code)/100)
        value = math.log(len(code))
        if rank < 5:
            print(name, "标的数量过少,不适合大模型策略")
        df = df[(df["真实价格"] >= 4)].copy()  # 过滤低价股
        df = df[(df["开盘收盘幅"] <= 0.08) & (
            df["开盘收盘幅"] >= -0.01)].copy()  # 过滤可能产生大回撤的股票
        m = 0.04
        n = 0.16
        w = m*value/rank  # 权重系数
        v = n*value/rank  # 权重系数
        num = rank  # 持仓数量
        df = df[(df["昨日资金波动_rank"] <= w)].copy()
        df = df[(df["昨日资金贡献_rank"] <= v)].copy()
        df = df.groupby(["日期"], group_keys=True).apply(
            lambda x: x.nlargest(num, "昨日资金波动")).reset_index(drop=True)
    return df


def technology(df):  # 定义计算技术指标的函数
    try:
        # df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        # 删除最高价和最低价为负值的数据
        df.drop(df[(df["最高"] < 0) | (df["最低"] < 0)].index, inplace=True)
        df.sort_values(by="日期")    # 以日期列为索引,避免计算错误
        # 定义开盘收盘幅
        df["开盘收盘幅"] = df["开盘"]/df["收盘"].copy().shift(1) - 1
        # 计算涨跌幅
        df["涨跌幅"] = df["收盘"]/df["收盘"].copy().shift(1) - 1
        # 计算昨日振幅
        df["昨日振幅"] = (df["最高"].copy().shift(
            1)-df["最低"].copy().shift(1))/df["开盘"].copy().shift(1)
        # 计算昨日成交额
        df["昨日成交额"] = df["成交额"].copy().shift(1)
        # 计算昨日涨跌
        df["昨日涨跌"] = df["涨跌幅"].copy().shift(1)+1
        # 计算昨日资金贡献
        df["昨日资金贡献"] = df["昨日涨跌"] / df["昨日成交额"]
        # 计算昨日资金波动
        df["昨日资金波动"] = df["昨日振幅"] / df["昨日成交额"]
        if ("分钟" in name) | ("指数" in name) | ("行业" in name):
            for n in range(1, 10):
                df[f"过去{n}日总涨跌"] = df["开盘"]/(df["开盘"].copy().shift(n))
                df[f"过去{n*5}日总涨跌"] = df["开盘"]/(df["开盘"].copy().shift(n*5))
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
    collection = db[f"实盘{name}"]
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
        df = choose("交易", name, df)
        print(df)
        if len(df) < 200:
            # 发布到钉钉机器人
            df["市场"] = f"实盘{name}"
            message = df[["市场", "代码", "日期", "开盘"]].to_markdown()
            print(type(message))
            webhook = "https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165"
            requests.post(webhook, json={"msgtype": "markdown", "markdown": {
                "title": f"{name}", "text": message}})
    except Exception as e:
        print(f"发生bug: {e}")


while True:
    client = MongoClient(
        "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
    db = client["wth000"]
    names = [("000", "001", "002", "600", "601", "603", "605")]
    # 获取当前日期，并通过akshare访问当日A股指数是否有数据，如果有数据则说明今日A股开盘，进行下一步的操作
    start_date = datetime.datetime.now().strftime("%Y-%m-%d")
    day = ak.index_zh_a_hist(symbol="000002", start_date=start_date, period="daily")
    if not day.notna().empty:
        timestamp = datetime.datetime.strptime(
            start_date, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()
        # 从akshare获取A股主板股票实时数据
        codes = ak.stock_zh_a_spot_em()
        # 过滤掉ST股票
        codes = codes[~codes["名称"].str.contains("ST")]
        # 过滤掉退市股票
        codes = codes[~codes["名称"].str.contains("退")]
        for name in names:
            try:
                codes = codes[codes["代码"].str.startswith(name)]
                codes["开盘"] = codes["今开"]
                codes["真实价格"] = codes["开盘"]
                codes["收盘"] = codes["最新价"]
                collection = db[f"实盘{name}"]
                latest = list(collection.find({"timestamp": timestamp}, {
                    "timestamp": 1}).sort("timestamp", -1).limit(1))
                print(latest)
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
                    codes["timestamp"] = timestamp
                    codes["日期"] = start_date
                    codes["代码"] = codes["代码"].apply(lambda x: float(x))
                    codes["成交量"] = codes["成交量"].apply(lambda x: float(x))
                    codes = codes.to_dict("records")
                    if upsert_docs:
                        print(f"新增数据")
                        try:
                            collection.insert_many(codes)
                        except Exception as e:
                            pass
                    else:
                        bulk_insert = []
                        for doc in codes:
                            print(doc["代码"], "数据更新")
                            if doc["timestamp"] > latest_timestamp:
                                # 否则，加入插入列表
                                bulk_insert.append(doc)
                            if doc["timestamp"] == float(latest_timestamp):
                                try:
                                    collection.update_many({"代码": doc["代码"], "timestamp": float(timestamp)}, {
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
                    print(e)
                    print("任务已经完成")
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
        for name in names:
            tradelist(name)
        time.sleep(43200)
