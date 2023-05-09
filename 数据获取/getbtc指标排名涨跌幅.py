import pandas as pd
import numpy as np
import datetime
import os

# 设置参数
name = 'BTC'
# name = 'COIN'
# name = '上证'
# name = '深证'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 去掉噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]

if 'btc' in name.lower():
    n = 15
if 'stock' in name.lower():
    n = 6
if 'coin' in name.lower():
    n = 6
mubiao = f'{n}日后总涨跌幅（未来函数）'

# 过滤符合条件的数据
if 'stock' in name.lower():
    df = df[df['真实价格'] >= 4].copy()
    df = df[df['开盘收盘幅'] <= 8].copy()
if 'coin' in name.lower():
    # 过滤低成交的垃圾股
    df = df[df['昨日成交额'] >= 1000000].copy()
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.00000500].copy()
if 'btc' in name.lower():
    # 过滤低成交的垃圾股
    df = df[df['昨日成交额'] >= 10000].copy()
    # 过滤条件(清算规避)：不在0、8、14整点时间之前20分钟的数据（时间戳对应的标准时间）
    df['资金结算'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df[df['资金结算'].apply(lambda x: not ((x.hour in [7, 15, 23]) and (x.minute > 40)))]

# 将数据划分成a个等长度的区间
a = 50
ranges = []
left = 0
right = 1
step = (right - left) / a
for i in range(a):
    ranges.append((left + i * step, left + (i + 1) * step))

# 筛选出列名中包含'rank'的列
rank_cols = df.filter(like='rank').columns.tolist()
# 创建空的结果DataFrame
result_df = pd.DataFrame()
# 循环处理每个指标和区间
for rank_range in ranges:
    col_result_df = pd.DataFrame()  # 创建一个空的DataFrame，用于存储指标的结果
    for col_name in rank_cols:
        # 根据区间筛选DataFrame
        sub_df = df[(df[col_name] >= rank_range[0]) &
                    (df[col_name] <= rank_range[1])]
        # 计算均值
        sub_df_mean = sub_df.mean(numeric_only=True)
        # 构造包含指标名和涨跌幅的DataFrame，并添加到列结果DataFrame中
        result_sub_df = pd.DataFrame(
            {col_name: [sub_df_mean[mubiao]]}, index=[rank_range])
        col_result_df = pd.concat([col_result_df, result_sub_df], axis=1)
    result_df = pd.concat([result_df, col_result_df])
    # print(result_df)
# 保存结果
result_df.to_csv(f'{name}_{n}日后指标排名区间涨跌幅.csv')
print('任务已经完成！')
