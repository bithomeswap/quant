import numpy as np
import pandas as pd

# 读取'STOCK指标.csv'文件
df = pd.read_csv('STOCK指标.csv')

# 对'MACDsignal'列进行排序
sorted_data = np.sort(df['MACDsignal'])

# 划分的区间数为6
n = 40

# 按照等距离的方式将数据划分成n个部分
indices = np.linspace(0, len(df['MACDsignal']), num=n+1, endpoint=True, dtype=int)

# 得到每一个区间的上界，并作为该部分对应的区间范围
ranges = []
for i in range(len(indices) - 1):
    start_idx = indices[i]
    end_idx = indices[i+1] if i != len(indices) - 2 else len(df['MACDsignal'])  # 最后一段需要特殊处理
    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
    ranges.append((sorted_data[start_idx], upper_bound))

# 输出结果
print(ranges)
