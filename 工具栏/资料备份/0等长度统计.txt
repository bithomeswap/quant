import time
from pymongo import MongoClient
import pandas as pd
import numpy as np

name = 'STOCK'
df = pd.read_csv('本地stock指标.csv')
mubiao = 'abs(MACD/MACDsignal-1)'
print('任务已经开始')
df = df.dropna()  # 删除含有空值的行

# df = df[df['MACD交叉状态'] < 0].copy()
df = df[df['MACD交叉状态'] > 0].copy()


# 去掉开盘幅过高的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]
# 去掉n日后总涨跌幅大于百分之三百的噪音数据

# 对指定列排序
sorted_data = np.sort(df[f'{mubiao}'])
# 将数据划分成n个等距离的区间
a = 21
indices = np.linspace(0, len(df[f'{mubiao}']),
                      num=a+1, endpoint=True, dtype=int)
# # 得到每一个区间的上界，并作为该部分对应的区间范围

df.round(decimals=6).to_csv(f'金叉样本分布.csv', index=False)

ranges = [(0, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8),
          (0.8, 0.9), (0.9, 1), (1, 1.1),(1.1, 1.2), (1.2, 1.3),(1.3,1.4), (1.4, 1.5),(1.5,1.6), 
          (1.6, 1.7),(1.7,1.8), (1.8, 1.9),(1.9,2.0), (2, 100)]

result_dicts = []

for n in range(1, 20):
    for rank_range in ranges:
        sub_df = df.copy()[(df[f'{mubiao}'] >= rank_range[0]) &
                           (df[f'{mubiao}'] <= rank_range[1])]
        # sub_df.round(decimals=6).to_csv(
        #     f'{name}标的{mubiao}因子{rank_range[0]}至{rank_range[1]}区间样本分布.csv', index=False)
        count = len(sub_df)
        future_returns = np.array(sub_df[f'{n}日后总涨跌幅（未来函数）'])
        # 括号注意大小写的问题，要不就会报错没这个参数
        up_rate = len(
            future_returns[future_returns >= 0]) / len(future_returns)
        avg_return = np.mean(future_returns)
        result_dict = {
            f'{mubiao}': f'from{rank_range[0]}to{rank_range[1]}',
            f'{n}日统计次数（已排除涨停）': count,
            f'未来{n}日上涨概率': up_rate,
            f'未来{n}日上涨次数': len(future_returns[future_returns >= 0]),
            f'未来{n}日平均涨跌幅': avg_return,
        }
        result_dicts.append(result_dict)
# 将结果持久化
result_df = pd.DataFrame(result_dicts)
for n in range(1, 20):
    cols_to_shift = [f'{n}日统计次数（已排除涨停）',
                     f'未来{n}日上涨概率', f'未来{n}日上涨次数', f'未来{n}日平均涨跌幅']
    result_df[cols_to_shift] = result_df[cols_to_shift].shift(-a*(n-1))

result_df = result_df.dropna()  # 删除含有空值的行
result_df.round(decimals=6).to_csv(
    f'金叉涨幅分布.csv', index=False
)
