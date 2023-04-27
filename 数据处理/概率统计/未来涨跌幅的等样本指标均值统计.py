import time
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime
import os

name = 'COIN'
# name = 'STOCK'
# name = 'BTC'
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
if 'stock' in name.lower():
    df = df[df['真实价格'] >= 4].copy()
    df = df[df['开盘收盘幅'] <= 8].copy()
if 'coin' in name.lower():
    df = df[df['昨日成交额'] >= 1000000].copy()
df = df[df['日期'] >= datetime.datetime(2022, 6, 1)]  # 仅保留从2020-01-01之后的数据

n = 9
mubiao = f'{n}日后总涨跌幅（未来函数）'
print('任务已经开始')
# 提取数值类型数据
numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
df = df[numerical_cols]
# 对指定列排序并去掉空值
df = df.dropna()
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
for col in df.columns:
    if col != '日期' and col != mubiao:  # 排除日期和涨跌幅列
        for rank_range in ranges:
            sub_df = df.copy()[(df[mubiao] >= rank_range[0])
                               & (df[mubiao] <= rank_range[1])]
            avg = np.mean(np.array(sub_df[col]))
            result_dict = {}
            result_dict['涨跌幅区间'] = f'from{rank_range[0]:.4f}to{rank_range[1]:.4f}'
            result_dict[f'{col}均值'] = avg
            result_dicts.append(result_dict)
# 将结果持久化
result_df = pd.DataFrame(result_dicts)
# print(result_df)
b = 0
for col in result_df.columns:
    b += 1
    if b>=3:
        result_df[f'{col}'] = result_df[f'{col}'].shift(-a*(b-2))
print(result_df)
# result_df = result_df.dropna()  # 删除含有空值的行
result_df.round(decimals=6).to_csv(
    f'等样本{name}标的{mubiao}的指标分布.csv', index=False)
