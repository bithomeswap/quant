import pandas as pd

# name = 'STOCK_20140101_20170101'
name = 'COIN'
# name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')
df_prod = pd.DataFrame()

# 指定需要计算的最高指标
n_list = ['160日最高开盘价比值', '40日最高开盘价比值', '160日最低开盘价比值', '40日最低开盘价比值']
for n in n_list:
    df_temp = df.groupby('日期')[f'{n}'].apply(
        lambda x: (x.mean())).reset_index(name=f'{n}')
    if df_prod.empty:
        df_prod = df_temp.copy()
    else:
        df_prod = pd.merge(df_prod, df_temp, on='日期')

# 计算多指标的平均值
df_prod['最高指标平均值'] = (df_prod['160日最高开盘价比值']+df_prod['40日最高开盘价比值'])/2
df_prod['最低指标平均值'] = (df_prod['160日最低开盘价比值']+df_prod['40日最低开盘价比值'])/2
df_prod['160指标'] = df_prod['160日最高开盘价比值']*df_prod['160日最低开盘价比值']
df_prod['40指标'] = df_prod['40日最高开盘价比值']*df_prod['40日最低开盘价比值']
df_prod['整体指标'] = df_prod['40日最高开盘价比值']*df_prod['160日最高开盘价比值']

# 输出到csv文件
df_prod.to_csv(f'{name}_多指标平均特征.csv', index=False)
