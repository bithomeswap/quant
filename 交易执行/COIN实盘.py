import requests
import time
import math
from binance.client import Client
from pymongo import MongoClient
import time
import pandas as pd


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
# 设置参数
name = "COIN"
collection = db[f"实盘{name}"]
# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
try:
    # 创建Binance客户端
    client = Client(api_key, api_secret)
    # 获取所有USDT计价的现货交易对
    ticker_prices = client.get_exchange_info()["symbols"]
    usdt_ticker_prices = [
        ticker_price for ticker_price in ticker_prices if ticker_price["quoteAsset"] == "USDT" and ("DOWN" not in ticker_price["symbol"]) and ("UP" not in ticker_price["symbol"])]
    print(f"当前币安现货有{len(ticker_prices)}个交易对")
    # 遍历所有现货交易对，并获取日K线数据
    for ticker_price in usdt_ticker_prices:
        symbol = ticker_price["symbol"]
        data_list = []
        # 找到该标的最新的时间戳
        latest_data = collection.find_one(
            {"代码": symbol}, {"timestamp": 1}, sort=[("timestamp", -1)])
        latest_timestamp = latest_data["timestamp"] if latest_data else 0
        klines = client.get_klines(
            symbol=symbol,
            interval=Client.KLINE_INTERVAL_1DAY,
            limit=3
        )
        # 插入到集合中
        for kline in klines:
            timestamp = kline[0] / 1000
            if timestamp < latest_timestamp:  # 如果时间戳小于等于最后时间戳，直接跳过
                continue
            date = time.strftime("%Y-%m-%d %H:%M:%S",
                                 time.gmtime(timestamp))
            if timestamp == latest_timestamp:
                update_data = {
                    "timestamp": timestamp,
                    "代码": symbol,
                    "日期": date,
                    "开盘": float(kline[1]),
                    "最高": float(kline[2]),
                    "最低": float(kline[3]),
                    "收盘": float(kline[4]),
                    "成交量": float(kline[5]),
                    "收盘timestamp": float(kline[6]/1000),
                    "成交额": float(kline[7]),
                }
                filter = {"代码": symbol, "timestamp": latest_timestamp}
                collection.update_one(
                    filter, {"$set": update_data})
            else:
                data_list.append({"timestamp": timestamp,
                                  "代码": symbol,
                                  "日期": date,
                                  "开盘": float(kline[1]),
                                  "最高": float(kline[2]),
                                  "最低": float(kline[3]),
                                  "收盘": float(kline[4]),
                                  "成交量": float(kline[5]),
                                  "收盘timestamp": float(kline[6]/1000),
                                  "成交额": float(kline[7]),
                                  })
        # 如果时间戳等于最新数据的时间戳，则执行更新操作，否则执行插入操作
        if len(data_list) > 0:
            collection.insert_many(data_list)
    print("任务已经完成")
    limit = 5000
    if collection.count_documents({}) >= limit:
        oldest_data = collection.find().sort([("日期", 1)]).limit(
            collection.count_documents({})-limit)
        ids_to_delete = [data["_id"] for data in oldest_data]
        collection.delete_many({"_id": {"$in": ids_to_delete}})
    print("数据清理成功")
except Exception as e:
    print(e)
time.sleep(1)
try:
    # 获取数据并转换为DataFrame格式
    df = pd.DataFrame(list(collection.find()))
    print(f"{name}数据读取成功")
    # 按照“代码”列进行分组并计算技术指标
    df = df.groupby(["代码"], group_keys=False).apply(technology)
    # 分组并计算指标排名
    df = df.groupby(["日期"], group_keys=False).apply(rank)
    df.sort_values(by="日期")    # 以日期列为索引,避免计算错误
    # 获取最后一天的数据
    last_day = df.iloc[-1]["日期"]
    # 计算总共统计的股票数量
    df = df[df[f"日期"] == last_day].copy()
    code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
    value = math.floor(math.log10(len(code)))  # 整数位数
    num = math.ceil(len(code)/(10**value))  # 持仓数量
    df = df[(df[f"开盘"] <= 10)&(df[f"开盘"] >= 0.00000500)].copy()  # 过滤低价股
    df = df[(df[f"过去{1}日资金波动_rank"] <= 0.01)].copy()
    # df = df.groupby(["日期"], group_keys=True).apply(lambda x: x.nsmallest(num, f"开盘_rank")).reset_index(drop=True)
    df = df.groupby(["日期"], group_keys=True).apply(lambda x: x.nsmallest(1, f"开盘_rank")).reset_index(drop=True)
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
