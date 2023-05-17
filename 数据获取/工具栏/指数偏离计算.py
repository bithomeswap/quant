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


name = "行业"
df = pd.DataFrame({"代码": ak.stock_board_industry_name_em()['板块名称']})


# name = "ETF"
# codelist = list(ak.stock_board_industry_name_ths()['name'])
# codelist = str(codelist).replace("'", "").replace('"', '').replace(",", "|")
# print(codelist)
# # 获取 A 股所有 ETF 基金代码
# df = ak.fund_etf_spot_em()
# df = df[~df['名称'].str.contains('港|纳|H|恒生|标普|黄金|货币|中概')]
# df = df[df['名称'].str.contains(f'{codelist}')]
# df = df[df['总市值'] >= 2000000000]

# name = "指数"
# df = pd.DataFrame({'代码':
#                    ['000002',
#                     # 中证行业指数
#                     '000986', '000987', '000988', '000989', '000990', '000991', '000992', '000993', '000994', '000995',
#                     # 深证行业指数
#                     '399613', '399614', '399615', '399616', '399617', '399618', '399619', '399620', '399621', '399622',
#                     # 上证等权指数
#                     '000070', '000071', '000072', '000073', '000074', '000075', '000076', '000077', '000078', '000079',
#                     ]
#                    })

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
ago = int((now - datetime.timedelta(days=1000)).timestamp())
data[['日期', '指数开盘']] = data[["日期", "开盘"]]
data = data[data['timestamp'] >= ago]
print(data)

n = 0
# 遍历目标指数代码，获取其分钟K线数据
for code in df['代码']:
    try:
        n += 1
        if name == 'COIN':
            etf = pd.DataFrame(list(collection.find(({"代码": str(code)}))))
            etf[['日期', f'{code}']] = etf[["日期", "开盘"]]
        elif name == '指数':
            etf = pd.DataFrame(list(collection.find(({"代码": float(code)}))))
            etf[['日期', f'{code}']] = etf[["日期", "开盘"]]
        elif name == '行业':
            etf = pd.DataFrame(list(collection.find(({"代码": str(code)}))))
            etf[['日期', f'{code}']] = etf[["日期", "真实价格"]]
        else:
            etf = pd.DataFrame(list(collection.find(({"代码": float(code)}))))
            etf[['日期', f'{code}']] = etf[["日期", "真实价格"]]
        etf = etf[etf['timestamp'] >= ago]
        if n == 1:
            df = pd.merge(data[['日期', '指数开盘']],
                          etf[['日期', f'{code}']], on='日期', how='left')
            df[f'指数偏离'] = df['指数开盘'] / (df["指数开盘"].iloc[0])
            df = df.drop(f'指数开盘', axis=1)
        if n > 1:
            df = pd.merge(df, etf[['日期', f'{code}']], on='日期', how='left')

        # # 起点对齐
        # df[f'{code}指数偏离'] = (
        #     df[f'{code}']/(df[f'{code}'].copy().iloc[0]))-df[f'指数偏离']

        # 终点对齐
        df[f'{code}指数偏离'] = (
            df[f'{code}']/(df[f'{code}'].copy().iloc[-1]))-df[f'指数偏离']
        df[f'{code}指数偏离差分'] = df[f'{code}指数偏离'] - \
            df[f'{code}指数偏离'].copy().shift(1)
        df = df.drop(f'{code}指数偏离', axis=1)

        df = df.drop(f'{code}', axis=1)
        print(df.loc[:0])
    except Exception as e:
        print(f"发生bug: {e}")
    if n == 300:
        break
# df = df.dropna(axis=1)  # 删除所有含有空值的列
df = df.fillna(0)

df = df.drop(f'指数偏离', axis=1)

# 绘图
# # 设置中文字体和短横线符号
# plt.rcParams['font.family'] = ['Microsoft YaHei']
# plt.rcParams['axes.unicode_minus'] = False
# plt.figure(figsize=(16, 8))
# plt.plot(df.set_index('日期'))
# plt.legend(df.columns.drop('日期'))
# plt.xlabel('日期')
# plt.ylabel('指数偏离度')
# plt.title(f'{n}种{name}指数对比')
# # plt.ylim(0)
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
