import pandas as pd

name = 'STOCK_20140101_20170101'
# name = 'COIN'
# name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')
# 特征名
n = 'EMA9开盘动能4'
# 统计所有交易日期各个标的的EMA121开盘比值的乘积
df_prod = df.groupby('日期')[f'{n}'].apply(
    lambda x: (x.mean())).reset_index(name=f'{n}')

# 计算SMA
sma_4d = df_prod[f'{n}'].rolling(window=4).mean()/df_prod[f'{n}']
sma_9d = df_prod[f'{n}'].rolling(
    window=9).mean()/df_prod[f'{n}'].rolling(window=4).mean()
sma_16d = df_prod[f'{n}'].rolling(
    window=16).mean()/df_prod[f'{n}'].rolling(window=9).mean()
sma_121d = df_prod[f'{n}'].rolling(
    window=121).mean()/df_prod[f'{n}'].rolling(window=16).mean()
# 合并数据
df_result = pd.concat(
    [df_prod['日期'], sma_4d, sma_9d, sma_16d, sma_121d], axis=1)

# 输出到csv文件
df_result.to_csv(f'{name}_{n}特征平均SMA.csv', index=False)
