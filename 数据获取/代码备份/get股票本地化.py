import akshare as ak
import pandas as pd
import datetime
import os
import pytz

names = [("000", "001", "002", "600", "601", "603", "605")]

# # 获取当前日期
# start_date = "20170101"
# current_date = datetime.datetime.now()
# end_date = current_date.strftime("%Y%m%d")
# 从akshare获取A股主板股票的代码和名称
codes = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
codes = codes[~codes["名称"].str.contains("ST")]
# 过滤掉退市股票
codes = codes[~codes["名称"].str.contains("退")]
for name in names:
    try:
        df = pd.DataFrame()
        df = codes[codes["代码"].str.startswith(name)][["代码", "名称"]].copy()
        # 遍历目标指数代码，获取其日K线数据
        for code in df["代码"]:
            # 通过 akshare 获取目标指数的日K线数据
            k_data = ak.stock_zh_a_hist(symbol=code, adjust="")
            # try:
            #     k_value = ak.stock_zh_valuation_baidu(symbol=code,  indicator="总市值", period="近五年")
            #     k_value.rename(columns={"date": "日期", "value": "总市值"}, inplace=True)
            #     k_value["日期"] = k_value["日期"].apply(lambda x: str(x))
            # except Exception as e:
            #     print(e, f"{code}百度基本面数据缺失")
            try:
                # k_data = pd.merge(k_data, k_value, on="日期", how="left")
                k_data["代码"] = float(code)
                k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
                k_data["timestamp"] = k_data["日期"].apply(lambda x: float(datetime.datetime.strptime(x, "%Y-%m-%d").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))
                # k_data["timestamp"] = k_data["日期"].apply(lambda x: float(datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone("Asia/Shanghai")).timestamp()))
                k_data = k_data.sort_values(by=["代码", "日期"])
                # 获取当前.py文件的绝对路径
                file_path = os.path.abspath(__file__)
                # 获取当前.py文件所在目录的路径
                dir_path = os.path.dirname(file_path)
                # 获取当前.py文件所在目录的上两级目录的路径
                dir_path = os.path.dirname(os.path.dirname(dir_path))
                # 保存数据到指定目录
                file_path = os.path.join(dir_path, f"{name}.csv")
                # 将最新数据插入到本地csv文件中
                with open(file_path, mode="a", encoding="utf-8", newline="") as f:
                    k_data.to_csv(f, header=f.tell() == 0, index=False)
            except Exception as e:
                print(e, f"因为{code}停牌")
        print("任务已经完成")
        # limit = 600000
        # if collection.count_documents({}) >= limit:
        #     oldest_data = collection.find().sort([("日期", 1)]).limit(
        #         collection.count_documents({})-limit)
        #     ids_to_delete = [data["_id"] for data in oldest_data]
        #     collection.delete_many({"_id": {"$in": ids_to_delete}})
        # print("数据清理成功")
    except Exception as e:
        print(e)
import os