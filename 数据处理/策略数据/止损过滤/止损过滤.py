import pandas as pd
import numpy as np
import math
import os

name = 'QSTOCK_20140101_20170101'
# name = 'COIN'
# name = 'STOCK'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上四级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(dir_path))))
# 保存数据到指定目录
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 假设需要在未来5天内对涨跌幅超过-5%的股票进行自动止损
n = 14  # 设置n为5，即判断未来五天内的总涨跌幅是否低于-5%
stop_loss = -5  # 设置止损线为-5%
# 对需要止损的股票进行自动止损
for order in df.index:
    for i in range(1, n):
        future_sum_return = df.at[order, f'{i}日后总涨跌幅（未来函数）']
        today_close_return = df.at[order, '涨跌幅']
        # 判断当日涨跌幅超过-8，无法止损卖出
        if today_close_return <= -8:
            continue
        if future_sum_return <= stop_loss:
            for j in range(i+1, n):
                df.at[order, f'{j}日后总涨跌幅（未来函数）'] = future_sum_return

print(f'各标的已设置回撤百分之{stop_loss}自动止损')
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上四级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(dir_path))))
file_path = os.path.join(dir_path, f'{name}止损指标.csv')
df.to_csv(file_path, index=False)
