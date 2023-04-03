from pymongo import MongoClient
import pandas as pd
import numpy as np

# 从本地CSV文件读取数据集合
name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')
mubiao = 'MACDsignal'
print('已经获取数据')


# 计算指标
for n in range(1, 8):
    rank_ranges = [(i-50.6, i-49.6) for i in range(100)]
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
