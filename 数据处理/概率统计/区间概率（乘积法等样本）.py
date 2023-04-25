import time
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime
import os

# name = 'COIN'
# name = 'STOCK'
name = 'BTC'
# name = '上证指数'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')  # 转换日期格式
df = df.loc[:, ~df.columns.str.contains('未来60日')]  # 去掉未来函数
if 'stock' in name.lower():
    df = df[df['真实价格'] >= 4].copy()
    df = df[df['开盘收盘幅'] <= 8].copy()
if 'coin' in name.lower():
    df = df[df['昨日成交额'] >= 1000000].copy()
df = df[df['日期'] >= datetime.datetime(2022, 6, 1)]  # 仅保留从2020-01-01之后的数据
mubiao = '10日最高开盘价比值'
print('任务已经开始')
# df = df.dropna(subset=[mubiao])
df = df.dropna()
df.to_csv(f"乘积法实际统计数据{name}_{mubiao}.csv")
# 对指定列排序
sorted_data = np.sort(df[f'{mubiao}'])

# 将数据划分成n个等距离的区间
a = 10
indices = np.linspace(0, len(df[f'{mubiao}']),
                      num=a+1, endpoint=True, dtype=int)
# 得到每一个区间的上界，并作为该部分对应的区间范围
ranges = []
for i in range(len(indices) - 1):
    start_idx = indices[i]
    end_idx = indices[i+1] if i != len(indices) - \
        2 else len(df[f'{mubiao}'])  # 最后一段需要特殊处理
    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
    ranges.append((sorted_data[start_idx], upper_bound))
result_dicts = []
for n in range(1, 11):
    for rank_range in ranges:
        sub_df = df.copy()[(df[f'{mubiao}'] >= rank_range[0]) &
                           (df[f'{mubiao}'] <= rank_range[1])]

        count = len(sub_df)
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
for n in range(1, 11):
    cols_to_shift = [f'{n}日统计次数（已排除涨停）',
                     f'未来{n}日上涨概率', f'未来{n}日上涨次数', f'未来{n}日平均涨跌幅']
    result_df[cols_to_shift] = result_df[cols_to_shift].shift(-a*(n-1))

# result_df = result_df.dropna()  # 删除含有空值的行

result_df.round(decimals=6).to_csv(
    f'乘积法等样本涨幅分布{name}标的{mubiao}.csv', index=False
)
