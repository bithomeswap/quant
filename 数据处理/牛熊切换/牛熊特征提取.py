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

# 获取自制成分股指数
code_count = len(df['代码'].drop_duplicates())
n_stock = code_count // 10
codes = df.groupby('代码')['成交额'].mean().nlargest(n_stock).index.tolist()
df = df[df['代码'].isin(codes)]
print("自制成分股指数为：", codes)

# 计算每个交易日的'SMA121开盘比值'均值
df_mean = df.groupby('日期')['SMA121开盘比值'].mean().reset_index(name='均值')

# 根据规则对每个交易日进行标注
df_mean['策略'] = df_mean['均值'].apply(lambda x: '震荡策略' if x >= 1 else '超跌策略')

# 输出到csv文件
df_mean.to_csv(f'{name}牛熊特征.csv', index=False)
