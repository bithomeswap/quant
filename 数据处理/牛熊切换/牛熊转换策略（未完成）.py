import math
import pandas as pd
import os

name = 'COIN'
# name = 'STOCK'
# name = 'COIN止损'
# name = 'STOCK止损'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

code_count = len(df['代码'].drop_duplicates())
print("标的数量", code_count)
n_stock = math.floor(code_count/10)

# 自制成分股指数,近期日均成交额的前百分之十为成分股指数
codes = df.groupby('代码')['成交额'].mean().nlargest(n_stock).index.tolist()

# 筛选出成分股并按日期排序
df = df[df['代码'].isin(codes)].sort_values('日期')

# 标记当日策略类型
sma121_mean = df.groupby('日期')['SMA121开盘比值'].mean()
df['策略类型'] = '趋势策略'
df.loc[sma121_mean[sma121_mean >= 1].index, '策略类型'] = '震荡策略'
df.loc[sma121_mean[sma121_mean < 1].index, '策略类型'] = '超跌策略'
df.to_csv("ceshi.csv")
