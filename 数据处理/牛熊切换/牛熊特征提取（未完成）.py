import pandas as pd

import os
# name = 'QSTOCK_20140101_20170101'
# name = 'COIN'
# name = 'STOCK'
# name = 'QSTOCK_20140101_20170101止损'
name = 'COIN止损'
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

df_prod = pd.DataFrame()

# 指定需要计算的最高指标
n_list = ['SMA9成交量比值', 'SMA121开盘比值', '160日最高开盘价比值',
          '40日最高开盘价比值', '160日最低开盘价比值', '40日最低开盘价比值']
for n in n_list:
    df_temp = df.groupby('日期')[f'{n}'].apply(
        lambda x: (x.mean())).reset_index(name=f'{n}')
    if df_prod.empty:
        df_prod = df_temp.copy()
    else:
        df_prod = pd.merge(df_prod, df_temp, on='日期')

# df_prod['复合指标'] = df_prod['40日最高开盘价比值']*df_prod['160日最高开盘价比值']

# 输出到csv文件
df_prod.to_csv(f'{name}_多指标平均特征.csv', index=False)
