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
import talib


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
for code in stock_info_df['代码']:
    if code.startswith(('60', '000', '001')):
        k_data = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date, end_date=end_date, adjust="hfq")  # 历史数据后复权，确保没未来函数
        k_data_true = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date, end_date=end_date, adjust="")  # 历史数据后复权，确保没未来函数
        try:
            # k_data['真实价格'] = k_data_true['开盘价']
            k_data_true = k_data_true[['日期', '开盘']].rename(
                columns={'开盘': '真实价格'})
            # 按照时间顺序合并
            k_data = pd.merge(k_data, k_data_true, on='日期', how='left')
            k_data['代码'] = float(code)
            k_data["成交量"] = k_data["成交量"].apply(lambda x: float(x))
            k_data['timestamp'] = k_data['日期'].apply(lambda x: float(
                datetime.datetime.strptime(x, '%Y-%m-%d').timestamp()))
            collection.insert_many(k_data.to_dict('records'))
        except:
            print(f"{name}({code}) 已停牌")
            continue
print('任务已经完成')
# # time.sleep(3600)