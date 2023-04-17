import pandas as pd

name = 'STOCK_20140101_20170101'
# name = 'COIN'
# name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')
# 特征名
n = 'SMA121开盘比值'
# 统计所有交易日期各个标的的SMA121开盘比值的平均值
df_prod = df.groupby('日期')[f'{n}'].apply(
    lambda x: (x.mean())).reset_index(name=f'{n}')
# 输出到csv文件
df_prod.to_csv(f'{name}_{n}特征平均.csv', index=False)
