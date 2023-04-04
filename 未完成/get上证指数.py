# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient, ASCENDING
import time

# 获取当前日期
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
# 读取180天内的数据，这里面还得排除掉节假日
date_ago = datetime.datetime.now() - datetime.timedelta(days=540)
start_date = date_ago.strftime('%Y%m%d')  # 要求格式"19700101"
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 输出的表为截止日期
name = '上证指数'
collection = db[f"{name}"]

print(f'开始日期{start_date}')
# 以日期为索引
# collection.create_index([('日期', ASCENDING)])

collection.drop()  # 清空集合中的所有文档

# 获取上证指数日k数据
klines = ak.index_zh_a_hist_min_em(symbol="000002",
                                   # 000001是上证指数，000002是A股指数
                                   period="5"
                                   # choice of {'1', '5', '15', '30', '60'} # 其中 1 分钟数据只能返回当前的, 其余只能返回近期的数据
                                   # start_date=start_date="1979-09-01 09:32:00"; 开始日期时间
                                   # end_date=end_date="2222-01-01 09:32:00"; 结束时间时间
                                   )

for kline in klines:
    # 将时间戳转换为ISO格式的日期字符串
    kline['日期'] = kline['时间']

    # 插入到数据库中
    if collection.count_documents({
        '日期': kline['时间'],
        '开盘': kline['开盘']
    }) == 0:
        collection.insert_one({
            '日期': kline['时间'],
            '开盘': kline['开盘'],
            '开盘': float(kline[1]),
            '最高': float(kline[2]),
            '最低': float(kline[3]),
            '收盘': float(kline[4]),
            '成交量': float(kline[5]),
            '收盘timestamp': float(kline[6]/1000),
            '成交额': float(kline[7]),
            '成交笔数': float(kline[8]),
            '主动买入成交量': float(kline[9]),
            '主动买入成交额':  float(kline[10])
        })

time.sleep(10)
# 将上证指数日K数据插入数据库
collection.insert_many(sh_data.to_dict('records'))
