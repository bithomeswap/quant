import pandas as pd
import numpy as np

# 读取数据
try:
    data = pd.read_csv('本地stock指标.csv')
except FileNotFoundError:
    print("文件不存在或者路径错误！")
    exit()  # 处理完错误退出程序

# 添加从数据库获取数据的提示
print('已经从数据库获取数据')

# 定义函数，用来筛选金叉区间内满足条件的数据


def select_data(df):
    # 对每只股票以时间升序排列
    df = df.sort_values(by='日期')
    # 筛选出所有金叉及死叉的索引位置，分别为 gold_cross_idx 和 dead_cross_idx
    gold_cross_idx = df.index[df['MACD交叉状态'] == 1]
    dead_cross_idx = df.index[df['MACD交叉状态'] == -1]

    # 遍历每次金叉，计算金叉区间的起始位置和终止位置
    golden_ranges = []
    for i in range(len(gold_cross_idx)):
        golden_start = gold_cross_idx[i]
        golden_end = dead_cross_idx[dead_cross_idx > golden_start].min()-1 if len(
            dead_cross_idx) > 0 else df.shape[0]
        golden_ranges.append((golden_start, golden_end))
    # 遍历每个金叉区间，在金叉区间内找到第一次满足条件 abs(MACD/MACDsignal-1) 大于 0.1 的行的索引，如果没有则返回 None
    filtered = []
    for start, end in golden_ranges:
        # 在金叉之后找到第一次满足条件 abs(MACD/MACDsignal-1) 大于 0.1 的行的索引，如果没有则返回 None
        try:
            filtered_idx = (
                df.loc[start:end, 'abs(MACD/MACDsignal-1)'] > 0.1).idxmax(skipna=True)
            if pd.isna(filtered_idx):
                filtered.append(None)
            else:
                # 返回金叉后的第一个满足条件的行
                filtered.append(df.loc[filtered_idx])
            # print(filtered)
        except ValueError:
            print(f"数据集 {df['代码'][start]} 中没有满足条件的行！")
    # print(filtered)
    return filtered


# 根据代码进行分组，并对每个分组应用 select_data 函数，最后将结果存储在名为filtered的列表中
filtered = []
for name, group in data.groupby('代码'):
    try:
        data_list = select_data(group)
        filtered.extend(data_list)
    except ValueError:
        print(f"数据集 {group['代码'].iloc[0]} 出现错误！")

if not filtered:
    print("没有符合要求的数据！")
    exit()  # 处理完错误退出程序

# 将筛选后的数据存储为CSV文件
result_df = pd.concat(
    [s for s in filtered if s is not None], axis=1).T
if result_df.empty:
    print("筛选后没有符合要求的数据！")
else:
    result_df.to_csv('金叉区间.csv', index=False)
    print('已完成任务！')
