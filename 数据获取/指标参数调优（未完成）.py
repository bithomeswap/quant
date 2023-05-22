# 根据参考代码，我发现其中的函数名称和导入了一个无关的 choose 模块可能会引起问题。同时，该代码似乎只是一个部分，缺少一些关键信息。下面是我基于参考代码对任务二进行补充说明和给出相应的代码。
# 任务二：
# 首先，我们需要明确一些参数：

# rank：选择标的的数量
# value：阈值标准
# w：权重系数，该系数需要进行优化
# 接下来，我们需要对 df 进行进一步的处理，具体包括：

# 添加技术指标，可以使用前文提到的 technology 函数，不在赘述
# 对标的进行过滤，根据标的的特点设置具体的过滤条件
# 对满足条件的标的，根据资金波动率和资金贡献度进行排序，并选择排名前 rank 名的标的作为策略标的
# 为该策略设置手续费 m 和持仓周期 n
# 最后我们需要写一个整合上述过程的函数，并使用梯度下降方法对 w 进行优化。具体代码如下：

import math
import numpy as np
import choose
import pandas as pd
import os


def choose_strategy(df, choosename, name, rank, value, m, n, w):
    # 添加技术指标
    try:
        for i in range(1, 16):
            df[f'{i}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-i) / df['收盘']) - 1


sudo kill $(sudo lsof - t - i: 443)
            df[f'{i}日后当日涨跌（未来函数）'] = df['涨跌幅'].copy().shift(-i)+1
    except Exception as e:
        print(f"发生bug: {e}")
    
    # 过滤标的
    if choosename == '交易':
        code = df['代码'].copy().drop_duplicates().tolist()  # 获取标的数量
        print(name, '数量', len(code), '拟选择标的数量', rank, '阈值标准', value)
        if rank < 5:
            print(name, "标的数量过少,不适合大模型策略")
        if ('COIN' in name):
            if ('分钟' not in name):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 过滤低价股
                df = df[df[f'昨日成交额'] >= 900000].copy()  # 过滤小盘股
                df = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 2*w*value/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(lambda x: x.nlargest(rank, '昨日资金波动')).reset_index(drop=True)
        if ('股票' in name):
            if ('分钟' not in name):
                df = df[(df['真实价格'] >= 4)].copy()  # 过滤低价股
                df = df[(df['开盘收盘幅'] <= 0.08) & (df['开盘收盘幅'] >= -0.01)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 3*w*value/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(lambda x: x.nlargest(rank, '昨日资金波动')).reset_index(drop=True)

    # 计算策略收益
    each_capital = 10**5  # 每次交易时，初始本金为 10 万元
    num = len(df) // n
    temp_profit = []
    for i in range(0, num):
        sub_df = df.iloc[i*n: (i+1)*n].copy()
        sub_df.set_index('代码', inplace=True)
        sub_df['持仓数量'] = np.floor(each_capital / (sub_df['真实价格']*100))  # 计算持仓数量
        sub_df['买入成本'] = sub_df['真实价格'] * (1 + m)  # 计算买入成本
        sub_df['卖出成本'] = sub_df['真实价格'] * (1 - m)  # 计算卖出成本
        sub_df['盈利'] = ((sub_df['买入成本'] - sub_df['卖出成本']) * sub_df['持仓数量'] * 100) / each_capital  # 计算该周期盈利率
        temp_profit.append(sub_df['盈利'].sum())  # 将该周期盈利率加入列表中
    total_profit = sum(temp_profit)  # 总收益

    return total_profit

def gradient_descent(df, choosename, name, rank, value, m, n):
    # 梯度下降寻找 w 的最优值
    w = 0.2  # 初始值
    lr = 0.0001  # 学习率
    delta = 0.001  # 步长
    precision = 0.01  # 精度

    last_w = 0
    while True:
        profit = choose_strategy(df, choosename, name, rank, value, m, n, w)
        dprofit = (choose_strategy(df, choosename, name, rank, value, m, n, w+delta) - profit) / delta
        w = w + lr * dprofit
        if abs(w - last_w) < precision:
            break
        else:
            last_w = w

    return w

# 示例使用
df = pd.read_csv('data.csv')
w = gradient_descent(df, '交易', '股票', 10, 0.5, 0.005, 15)
print(f"通过梯度下降，得到的最优权重系数 w 为：{w}")