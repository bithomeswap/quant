import time
from pymongo import MongoClient
import pandas as pd
import numpy as np

name = 'COIN'
df = pd.read_csv(f'{name}指标.csv')
mubiao = '开盘'
print('任务已经开始')
df = df.dropna()  # 删除含有空值的行

df = df[df['开盘幅'] <= 9.9].copy()
# 开盘幅过滤
# df = df[df['SMA121收盘比值'] <= 0.8].copy()
# df = df[df['SMA121开盘比值'] <= 0.8].copy()
# df = df[df['SMA121最高比值'] <= 0.8].copy()
# df = df[df['SMA121最低比值'] <= 0.8].copy()

# # 四均线过滤STOCK0.8
df = df[df['SMA121收盘比值'] <= 0.5].copy()
df = df[df['SMA121开盘比值'] <= 0.5].copy()
df = df[df['SMA121最高比值'] <= 0.5].copy()
df = df[df['SMA121最低比值'] <= 0.5].copy()
# 四均线过滤COIN0.5

# df = df[df['换手率'] <= 3.3].copy()
# 换手率过滤（仅限A股）

# 去掉开盘幅过高的噪音数据
for n in range(1, 10):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]
# 去掉n日后总涨跌幅大于百分之三百的噪音数据

# 对指定列排序
sorted_data = np.sort(df[f'{mubiao}'])
# 将数据划分成n个等距离的区间
a = 40
indices = np.linspace(0, len(df[f'{mubiao}']),
                      num=a+1, endpoint=True, dtype=int)
# 得到每一个区间的上界，并作为该部分对应的区间范围
df.round(decimals=6).to_csv(f'{name}标的{mubiao}因子样本分布.csv', index=False)
ranges = []
for i in range(len(indices) - 1):
    start_idx = indices[i]
    end_idx = indices[i+1] if i != len(indices) - \
        2 else len(df[f'{mubiao}'])  # 最后一段需要特殊处理
    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
    ranges.append((sorted_data[start_idx], upper_bound))
result_dicts = []

for n in range(1, 10):
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
for n in range(1, 10):
    cols_to_shift = [f'{n}日统计次数（已排除涨停）',
                     f'未来{n}日上涨概率', f'未来{n}日上涨次数', f'未来{n}日平均涨跌幅']
    result_df[cols_to_shift] = result_df[cols_to_shift].shift(-a*(n-1))

result_df = result_df.dropna()  # 删除含有空值的行
result_df.round(decimals=6).to_csv(
    f'{name}标的{mubiao}涨幅分布.csv', index=False
)
