# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient
import time


client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 输出的表为截止日期
name = 'STOCK实盘'
collection = db[f"{name}"]
collection.drop()  # 清空集合中的所有文档
# 获取当前日期
current_date = datetime.datetime.now()
# 读取180天内的数据，这里面还得排除掉节假日
date_ago = current_date - datetime.timedelta(days=250)
start_date = date_ago.strftime('%Y%m%d')  # 要求格式"19700101"
end_date = current_date.strftime('%Y%m%d')
# 从akshare获取A股主板股票的代码和名称
stock_info_df = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('ST')]
# 过滤掉退市股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('退')]
# 迭代每只股票，获取每天的前复权日k数据
data_list = []
for code in stock_info_df['代码']:
    if code.startswith(('60', '000', '001')):
        k_data = ak.stock_zh_a_hist(
            # symbol=code, start_date=start_date, end_date=end_date, adjust="hfq")
            # 历史数据后复权，确保没负数
            symbol=code, start_date=start_date, end_date=end_date, adjust="qfq")
        # 近期数据前复权，确保真数据
        try:
            k_data['代码'] = float(code)
            k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
            k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
                datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
            data_list.append(k_data)
        except:
            print(f"{name}({code}) 已停牌")
            continue
print('任务已经完成')

klines = ak.index_zh_a_hist(
    symbol="000001", period="daily", start_date=start_date, end_date=end_date)
klines['代码'] = float(000000)
klines["成交量"] = klines["成交量"].apply(lambda x: float(x))
klines['timestamp'] = klines['日期'].apply(lambda x: float(
    datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
data_list.append(klines)

collection.insert_many(data_list.to_dict('records'))

# def get_technical_indicators(df):  # 定义计算技术指标的函数
#     # 过滤最高价和最低价为负值的数据
#     df = df.loc[(df['最高'] >= 0) & (df['最低'] >= 0)]
#     df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

#     # 计算昨日成交额
#     df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
#     # 定义开盘收盘幅
#     df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
#     # 定义收盘幅即涨跌幅
#     df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100
#     df = df.dropna()  # 删除缺失值，避免无效数据的干扰
#     for n in range(1, 14):  # 计算未来n日涨跌幅
#         df[f'{n*10}日最低开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).min()
#         df[f'SMA{n*10}开盘比值'] = df['开盘'] / \
#             talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
#     return df


# # 按照“代码”列进行分组并计算技术指标
# grouped = data_list.groupby('代码').apply(get_technical_indicators)
# print('准备插入数据')
# # 连接MongoDB数据库并创建新集合
# new_collection = db[f'{name}指标']
# new_collection.drop()  # 清空集合中的所有文档
# # 将数据分批插入
# batch_size = 5000  # 批量插入的大小
# num_batches = len(grouped) // batch_size + 1
# for i in range(num_batches):
#     start_idx = i * batch_size
#     end_idx = (i + 1) * batch_size
#     data_slice = grouped[start_idx:end_idx]
#     new_collection.insert_many(data_slice.to_dict('records'))
