# 使用等距离的方式进行数据划分。可以使用NumPy中的linspace函数，将数据集的最大值和最小值之间的区间划分为n个部分。
# 具体实现代码如下：
import numpy as np
# 假设数据存储在ndarray类型的变量data中
data = np.array([1, 3, 5, 6, 8, 9, 10, 12, 15, 18])
n = 5  # 划分的区间数
# 对数据进行排序
sorted_data = np.sort(data)
# 按照等距离的方式将数据划分成n个部分
indices = np.linspace(0, len(data), num=n+1, endpoint=True, dtype=int)
# 得到每一个区间的上界，并作为该部分对应的区间范围
ranges = []
for i in range(len(indices) - 1):
    start_idx = indices[i]
    end_idx = indices[i+1] if i != len(indices) - 2 else len(data)  # 最后一段需要特殊处理
    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
    ranges.append((sorted_data[start_idx], upper_bound))
# 输出结果
print(ranges)