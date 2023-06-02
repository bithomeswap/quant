import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
names = [("000", "001", "002", "600", "601", "603", "605")]

# 获取当前日期
start_date = "20170101"
# current_date = datetime.datetime.now()
# end_date = current_date.strftime("%Y%m%d")
# 从akshare获取A股主板股票的代码和名称
codes = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
codes = codes[codes["名称"].str.contains("ST")]
for name in names:
    try:
        df = codes[codes["代码"].str.startswith(name)][["代码", "名称"]].copy()
        # 遍历目标指数代码，获取其日K线数据
        for code in df["代码"]:
            st_date_df = ak.stock_financial_report_sina(stock=code,  quarter=1, type='report')
            start_date = st_date_df.loc[st_date_df['公司类型'] == 'ST', '报表日期'].min()  # ST 开始日期
            end_date = st_date_df.loc[st_date_df['公司类型'].isna(),'报表日期'].min()  # ST 结束日期
        # 打印结果
        print(df)
    except Exception as e:
        print(e)