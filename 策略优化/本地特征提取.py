# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
import numpy as np
import pandas as pd
import talib
import os
from pymongo import MongoClient

# 选择要分析的产品
# name = "COIN"
name = "STOCK"

client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
file_path = os.path.join(dir_path, f'{name}指标.csv')

df = pd.read_csv(file_path)
print('数据获取成功')

# 提取数值类型数据
numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
df_numerical = df[numerical_cols]
db[f'{name}待训练'].drop()  # 清空集合中的所有文档
db[f'{name}待训练'].insert_many(df_numerical.to_dict('records'))
# # 计算相关系数,第一个参数是相关性分析的方法（这个是非线性和线性一起算）,第二个参数是只计算数字列
corr_df = df_numerical.corr(method='spearman')
# # 计算相关系数,第一个参数是相关性分析的方法（这个是只计算线性相关一起算）,第二个参数是只计算数字列
# corr_df = df_numerical.corr(method='pearson')

# 将相关系数数据保存到名为'相关系数'的数据集合中
corr_dict = corr_df.round(decimals=6).to_dict('index')
db[f'{name}相关系数'].drop()  # 清空集合中的所有文档
for col, value in corr_dict.items():
    value['_id'] = col
    db[f'{name}相关系数'].insert_one(value)
