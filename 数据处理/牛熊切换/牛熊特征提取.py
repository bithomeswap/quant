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

code_count = len(df['代码'].drop_duplicates())
print("标的数量", code_count)
n_stock = math.floor(code_count/10)

# 自制成分股指数,近期日均成交额的前百分之十为成分股指数
codes = df.groupby('代码')['成交额'].mean().nlargest(n_stock).index.tolist()
df = df[df['代码'].isin(codes)]
print(f"{name}成分股为", codes)

df_prod = pd.DataFrame()

# 指定需要计算的最高指标
n_list = [
    'SMA121开盘比值',
]

for n in n_list:
    df_temp = df.groupby('日期')[f'{n}'].apply(
        lambda x: (x.mean())).reset_index(name=f'{n}')
    if df_prod.empty:
        df_prod = df_temp.copy()
    else:
        df_prod = pd.merge(df_prod, df_temp, on='日期')

# 输出到csv文件
df_prod.to_csv(f'{name}_多指标平均特征.csv', index=False)
