from pymongo import MongoClient
import pandas as pd
import numpy as np

# 从本地CSV文件读取数据集合
name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')
mubiao = 'MACDsignal'
print('已经获取数据')

# 对'MACDsignal'列进行排序
sorted_data = np.sort(df['MACDsignal'])

n = 40  # 划分的区间数为40
# 按照等距离的方式将数据划分成n个部分
indices = np.linspace(0, len(df['MACDsignal']),
                      num=n+1, endpoint=True, dtype=int)
# 得到每一个区间的上界，并作为该部分对应的区间范围
ranges = []
for i in range(len(indices) - 1):
    start_idx = indices[i]
    end_idx = indices[i+1] if i != len(indices) - \
        2 else len(df['MACDsignal'])  # 最后一段需要特殊处理
    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
    ranges.append((sorted_data[start_idx], upper_bound))

# 计算指标
for n in range(1, 8):
    # 将划分的区间赋值到待计算的列表当中
    rank_ranges = ranges
    result_dicts = []
    for rank_range in rank_ranges:
        sub_df = df[df[f'{mubiao}'].between(
            rank_range[0], rank_range[1])]
        # 过滤当日涨停无法交易的情况
        sub_df = sub_df[sub_df[f'{n}日后总涨跌幅（未来函数）'] < 10]
        count = len(sub_df)
        future_returns = sub_df[[f'{n}日后总涨跌幅（未来函数）']].values
        up_rate = len(
            future_returns[future_returns >= 0])/count if count != 0 else 0
        avg_return = np.mean(sub_df[[f'{n}日后总涨跌幅（未来函数）']].values)
        is_limit_up_count = len(sub_df[sub_df['是否涨跌停'] == 1])
        result_dict = {
            f'{mubiao}': f'from{rank_range[0]}to{rank_range[1]}',
            '总统计次数（已排除涨停）': count,
            f'未来{n}日上涨概率': up_rate,
            f'未来{n}日上涨次数': len(future_returns[future_returns >= 0]),
            f'未来{n}日平均涨跌幅': avg_return,
        }
        result_dicts.append(result_dict)

    # 将结果存入数据库
    result_df = pd.DataFrame(result_dicts)
    result_df.round(decimals=6).to_csv(
        f'{name}标的{mubiao}{n}日涨幅分布.csv', index=True)
