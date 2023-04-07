
import time
from pymongo import MongoClient
import pandas as pd
import numpy as np
# 使用等距离的方式进行数据划分。可以使用NumPy中的linspace函数，将数据集的最大值和最小值之间的区间划分为n个部分。
# 具体实现代码如下：
# 从本地CSV文件读取数据集合
name = 'STOCk'
df = pd.read_csv(f'{name}指标.csv')
mubiao = 'KDJ_D'
print('已经获取数据')
# # 对MACDsignal在-0.7至-0.03之间的数据进行预处理
# gongzhen = 'MACDsignal'
# df = df[(df[f'{gongzhen}'] >= -0.7) & (df[f'{gongzhen}'] <= -0.03)]

# # 对换手率在0.23-0.67之间的数据进行预处理
# gongzhen = '换手率'
# df = df[(df[f'{gongzhen}'] <= 0.65)&(df[f'{gongzhen}'] >= 0.23)]

# # 对KDJ_D在<7的数据进行预处理
# gongzhen = 'KDJ_D'
# df = df[(df[f'{gongzhen}'] <= 7)]


# 对指定列排序
sorted_data = np.sort(df[f'{mubiao}'])
# 将数据划分成n个等距离的区间
a = 40
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

# 计算指标
for n in range(1, 10):
    rank_ranges = ranges

    for rank_range in rank_ranges:
        sub_df = df[df[f'{mubiao}'].between(
            rank_range[0], rank_range[1])]

        # 过滤到高开买不到的情况
        sub_df = sub_df[sub_df[f'开盘幅'] < 5]
        sub_df = sub_df[sub_df[f'开盘幅'] > -5]

        count = len(sub_df)
        future_returns = sub_df[[f'{n}日后总涨跌幅（未来函数）']].values
        up_rate = len(
            future_returns[future_returns >= 0])/count if count != 0 else 0
        avg_return = np.mean(sub_df[[f'{n}日后总涨跌幅（未来函数）']].values)
        is_limit_up_count = len(sub_df[sub_df['是否涨跌停'] == 1])
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
