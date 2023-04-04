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
sh_data = ak.stock_zh_index_daily_em(symbol="sh000001")
# 获取深证指数日k数据
sh_data['代码'] = "sh000001"
sh_data['名称'] = "上证指数"
sh_data['日期'] = sh_data["date"]
sh_data['开盘'] = sh_data["open"]
sh_data['收盘'] = sh_data["close"]
sh_data['最高'] = sh_data["high"]
sh_data['最低'] = sh_data["low"]
sh_data['成交量'] = sh_data["volume"].astype(float)
sh_data['成交额'] = sh_data["amount"]
sh_data = sh_data.drop(["date", "open", "close", "high",
                       "low", "volume", "amount"], axis=1)
# 将上证指数日K数据插入数据库
collection.insert_many(sh_data.to_dict('records'))
