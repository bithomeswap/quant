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




# 计算SMA
sma_4d = df_prod[f'{n}'].rolling(window=4).mean()/df_prod[f'{n}']
sma_9d = df_prod[f'{n}'].rolling(
    window=9).mean()//df_prod[f'{n}']
sma_16d = df_prod[f'{n}'].rolling(
    window=16).mean()/df_prod[f'{n}']
sma_121d = df_prod[f'{n}'].rolling(
    window=121).mean()/df_prod[f'{n}']
# 合并数据
df_result = pd.concat(
    [df_prod['日期'], sma_4d, sma_9d, sma_16d, sma_121d], axis=1)

# 过去二十个区间没有1以上的数据才判断熊市结束执行震荡策略，只要产生1以上的数据证明杀跌来临执行超跌策略

# 输出到csv文件
df_result.to_csv(f'{name}_{n}特征平均.csv', index=False)
