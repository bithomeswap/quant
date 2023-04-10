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
df = df[df['EMA121收盘比值'] <= 0.5].copy()
df = df[df['EMA121开盘比值'] <= 0.5].copy()
df = df[df['EMA121最高比值'] <= 0.5].copy()
df = df[df['EMA121最低比值'] <= 0.5].copy()
# 四均线过滤COIN0.5
# df = df[df['DIF收盘_4_9'] >= 0.16].copy()
# 动能强度过滤COIN0.16
df = df[df['开盘'] <= 0.9].copy()
# 开盘价过滤COIN0.9
df = df[df['开盘幅'] <= 9.9].copy()
df = df[df['开盘幅'] >= -0.01].copy()
# 开盘幅过滤COIN-0.01~9.9
print('数据预处理成功')
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
