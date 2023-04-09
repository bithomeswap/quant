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

# 沪市主板股票代码：600、601、603、605开头
# 深市主板股票代码：000、001开头
# 这里只录入了主板的数据，其他板块直接过滤掉了

# 获取当前日期
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
# 读取180天内的数据，这里面还得排除掉节假日
date_ago = datetime.datetime.now() - datetime.timedelta(days=1080)
start_date = date_ago.strftime('%Y%m%d')  # 要求格式"19700101"
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 输出的表为截止日期
name = 'STOCK'
collection = db[f"{name}"]
collection.drop()  # 清空集合中的所有文档

# 从akshare获取A股主板股票的代码和名称
stock_info_df = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('ST')]
# 迭代每只股票，获取每天的前复权日k数据
for code in stock_info_df['代码']:
    if code.startswith(('600', '601', '603', '605', '000', '001')):
        k_data = ak.stock_zh_a_hist(
            symbol=code, start_date=start_date, adjust="qfq")
        
        k_data['代码'] = float(code)

        k_data['成交量'] = k_data['成交量'].astype(float)
        # 将数据插入MongoDB，如果已经存在相同时间戳和内容的数据则跳过
        collection.insert_many(k_data.to_dict('records'))

print('任务已经完成')
# time.sleep(3600)
# limit = 400000
# if collection.count_documents({}) >= limit:
#     oldest_data = collection.find().sort([('日期', 1)]).limit(
#         collection.count_documents({})-limit)
#     ids_to_delete = [data['_id'] for data in oldest_data]
#     collection.delete_many({'_id': {'$in': ids_to_delete}})
# print('数据清理成功')
