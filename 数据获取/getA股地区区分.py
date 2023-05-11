import requests
import re
import akshare as ak
import pandas as pd

# 获取不同地区板块的股票代码信息
stock_sector_df = ak.stock_board_industry_cons_em()
ak.stock_board_industry_cons_em


# # 将信息按照板块分类
# stock_sector_dict = dict()
# for sector, df in stock_sector_df.groupby('sector_name'):
#     stock_sector_dict[sector] = df['code'].tolist()

print(stock_sector_df)
# # 遍历股票代码信息，将每个板块的数据插入数据库中（假设使用 MySQL 数据库）
# from sqlalchemy import create_engine
# import concurrent.futures
# from threading import Lock

# engine = create_engine('mysql+pymysql://username:password@localhost:port/database')
# lock = Lock()

# def insert_data(code):
#     # 这里可以使用 akshare 提供的股票数据接口获取股票数据
#     # 比如 ak.stock_zh_a_daily() 函数获取 A 股每日行情数据
#     # 这里以示例为主，不再实现获取股票数据
#     pass

# for sector,codes in stock_sector_dict.items():
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(insert_data, code) for code in codes]
#         for future in concurrent.futures.as_completed(futures):
#             try:
#                 # 在这里可以处理每个任务的返回结果
#                 with lock:
#                     print(future.result())
#             except Exception as e:
#                 with lock:
#                     print(e)
#                     continue


def stock_board_industry_name_em() -> pd.DataFrame:
    """
    东方财富网-沪深板块-行业板块-名称
    https://quote.eastmoney.com/center/boardlist.html#industry_board
    :return: 行业板块-名称
    :rtype: pandas.DataFrame
    """
    url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "2000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2 f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222",
        "_": "1626075887768",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "排名",
        "-",
        "最新价",
        "涨跌幅",
        "涨跌额",
        "-",
        "_",
        "-",
        "换手率",
        "-",
        "-",
        "-",
        "板块代码",
        "-",
        "板块名称",
        "-",
        "-",
        "-",
        "-",
        "总市值",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "上涨家数",
        "下跌家数",
        "-",
        "-",
        "-",
        "领涨股票",
        "-",
        "-",
        "领涨股票-涨跌幅",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "排名",
            "板块名称",
            "板块代码",
            "最新价",
            "涨跌额",
            "涨跌幅",
            "总市值",
            "换手率",
            "上涨家数",
            "下跌家数",
            "领涨股票",
            "领涨股票-涨跌幅",
        ]
    ]
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df["上涨家数"] = pd.to_numeric(temp_df["上涨家数"], errors="coerce")
    temp_df["下跌家数"] = pd.to_numeric(temp_df["下跌家数"], errors="coerce")
    temp_df["领涨股票-涨跌幅"] = pd.to_numeric(temp_df["领涨股票-涨跌幅"], errors="coerce")
    return temp_df
