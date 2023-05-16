import matplotlib.pyplot as plt
import os
import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time
import pytz

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]

name = "指数"
collection = db[f"{name}"]
data = pd.DataFrame(list(collection.find({"代码": float('000002')})))
name = "ETF"

# name = "COIN"
# collection = db[f"{name}"]
# data = pd.DataFrame(list(collection.find({"代码": str('BTCUSDT')})))
# name = "COIN"
# df = pd.DataFrame(
#     {'代码': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'TRXUSDT', 'OMGUSDT',
#             'BANDUSDT', 'PEOPLEUSDT', 'QKCUSDT', 'ELFUSDT', ' ICPUSDT',
#             'CKBUSDT', 'FIROUSDT']})

collection = db[f"{name}"]
# 计算一年前的日期
now = datetime.datetime.now()
one_year_ago = int((now - datetime.timedelta(days=365)).timestamp())
data[['日期', '指数开盘']] = data[["日期", "开盘"]]
data = data[data['timestamp'] >= one_year_ago]
# 输出结果
print(data)

if name == "ETF":
    df = ak.fund_etf_spot_em()
if '证' in name.lower():
    # 从akshare获取A股主板股票的代码和名称
    df = ak.stock_zh_a_spot_em()
    # 过滤掉ST股票
    df = df[~df['名称'].str.contains('ST')]
    # 过滤掉退市股票
    df = df[~df['名称'].str.contains('退')]
    if '深证' in name.lower():
        df = df[df['代码'].str.startswith(('000', '001'))][[
            '代码', '名称']]  # 获取上证的前复权日k数据
    if '上证' in name.lower():
        df = df[df['代码'].str.startswith(('600', '601'))][[
            '代码', '名称']]  # 获取深证的前复权日k数据

n = 0
# 遍历目标指数代码，获取其分钟K线数据
for code in df['代码']:
    try:
        n += 1
        if name == 'COIN':
            etf = pd.DataFrame(list(collection.find(({"代码": code}))))
            etf[['日期', f'{code}']] = etf[["日期", "开盘"]]
        else:
            etf = pd.DataFrame(list(collection.find(({"代码": float(code)}))))
            etf[['日期', f'{code}']] = etf[["日期", "真实价格"]]
        if n == 1:
            df = pd.merge(data[['日期', '指数开盘']],
                          etf[['日期', f'{code}']], on='日期', how='left')
            df[f'指数偏离'] = df['指数开盘'] / df["指数开盘"].dropna().iloc[-1]
        if n > 1:
            df = pd.merge(df, etf[['日期', f'{code}']], on='日期', how='left')
        df[f'{code}指数偏离'] = (
            df[f'{code}'] / df[f'{code}'].dropna().iloc[-1])/df[f'指数偏离']
        df = df.drop(f'{code}', axis=1)
        print(df)
    except Exception as e:
        print(f"发生bug: {e}")
    if n == 300:
        break
df = df.drop(f'指数开盘', axis=1)

# # 绘图
# # 设置中文字体和短横线符号
# plt.rcParams['font.family'] = ['Microsoft YaHei']
# plt.rcParams['axes.unicode_minus'] = False
# plt.figure(figsize=(16, 8))
# plt.plot(df.set_index('日期'))
# plt.legend(df.columns.drop('日期'))
# plt.xlabel('日期')
# plt.ylabel('指数偏离度')
# plt.title(f'{n}种{name}指数对比')
# plt.ylim(0)
# plt.show()

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f'{name}对比.csv')
df.to_csv(file_path, index=False)
