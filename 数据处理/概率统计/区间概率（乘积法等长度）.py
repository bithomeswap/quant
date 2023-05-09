import time
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime
import os
# 设置参数
# name = 'BTC'
# name = 'COIN'
# name = '上证'
name = '深证'
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
for n in range(1, 5):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]

df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')  # 转换日期格式
df = df.loc[:, ~df.columns.str.contains('未来60日')]  # 去掉未来函数
if 'stock' in name.lower():
    df = df[df['真实价格'] >= 4].copy()
    df = df[df['开盘收盘幅'] <= 8].copy()
if 'coin' in name.lower():
    df = df[df['昨日成交额'] >= 1000000].copy()
df = df[df['日期'] >= datetime.datetime(2022, 6, 1)]  # 仅保留从2020-01-01之后的数据
mubiao = f'开盘'
print('任务已经开始')
# df = df.dropna(subset=[mubiao])
df = df.dropna()
df.to_csv(f"实际统计数据{name}_{mubiao}.csv")
# 对指定列排序
sorted_data = np.sort(df[f'{mubiao}'])

# 将数据划分成n个等长度的区间
a = 10

ranges = []
left = 1
right = 10
step = (right - left) / a

for i in range(a):
    ranges.append((left + i * step, left + (i + 1) * step))
result_dicts = []
for n in range(1, 10):
    for rank_range in ranges:
        sub_df = df.copy()[(df[f'{mubiao}'] >= rank_range[0]) & (
            df[f'{mubiao}'] <= rank_range[1])]

        count = len(sub_df)
        if count == 0:
            result_dict = {
                f'{mubiao}': f'from{rank_range[0]}to{rank_range[1]}',
                f'{n}日统计次数（已排除涨停）': 0,
                f'未来{n}日上涨概率': 0,
                f'未来{n}日上涨次数': 0,
                f'未来{n}日平均涨跌幅': 0,
            }
        else:
            future_returns = np.array(sub_df[f'{n}日后总涨跌幅（未来函数）'])
            # 括号注意大小写的问题，要不就会报错没这个参数
            up_rate = len(
                future_returns[future_returns >= 0]) / len(future_returns)
            avg_return = np.prod(np.array(future_returns)+1)
            result_dict = {
                f'{mubiao}': f'from{rank_range[0]}to{rank_range[1]}',
                f'{n}日统计次数（已排除涨停）': count,
                f'未来{n}日上涨概率': up_rate,
                f'未来{n}日上涨次数': len(future_returns[future_returns >= 0]),
                f'未来{n}日平均涨跌幅': avg_return,
            }
            print(f'未来{n}日平均涨跌幅', f": {avg_return}")
        result_dicts.append(result_dict)

# 将结果持久化
result_df = pd.DataFrame(result_dicts)
for n in range(1, 10):
    cols_to_shift = [f'{n}日统计次数（已排除涨停）',
                     f'未来{n}日上涨概率', f'未来{n}日上涨次数', f'未来{n}日平均涨跌幅']
    result_df[cols_to_shift] = result_df[cols_to_shift].shift(-a*(n-1))

result_df = result_df.dropna()  # 删除含有空值的行

result_df.round(decimals=6).to_csv(
    f'等长度涨幅分布{name}标的{mubiao}.csv', index=False
)
