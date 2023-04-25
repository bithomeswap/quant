# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import numpy as np
import pandas as pd
from pymongo import MongoClient
# 链接mongodb数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 选择要分析的产品
name = "COIN"
collection = db[f'{name}指标']
print('数据库已链接')
df = pd.DataFrame(list(collection.find()))
print('数据获取成功')
if 'stock' in name.lower():
    df = df[df['真实价格'] >= 4].copy()
    df = df[df['开盘收盘幅'] <= 8].copy()
if 'coin' in name.lower():
    df = df[df['昨日成交额'] <= 1000000].copy()
# 提取数值类型数据
numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
df_numerical = df[numerical_cols]
db[f'{name}待训练'].drop()  # 清空集合中的所有文档
db[f'{name}待训练'].insert_many(df_numerical.to_dict('records'))
# # 计算相关系数,第一个参数是相关性分析的方法（这个是非线性和线性一起算）,第二个参数是只计算数字列
corr_df = df_numerical.corr(method='spearman', numeric_only=True)
# # 计算相关系数,第一个参数是相关性分析的方法（这个是只计算线性相关一起算）,第二个参数是只计算数字列
# corr_df = df_numerical.corr(method='pearson')
# 将相关系数数据保存到名为'相关系数'的数据集合中
corr_dict = corr_df.round(decimals=6).to_dict('index')
db[f'{name}相关系数'].drop()  # 清空集合中的所有文档
for col, value in corr_dict.items():
    value['_id'] = col
    db[f'{name}相关系数'].insert_one(value)
