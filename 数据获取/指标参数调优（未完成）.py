# rank：选择标的的数量
# value：阈值标准
# w：权重系数，该系数需要进行优化
# 为该策略设置手续费 m 和持仓周期 n
# 最后我们需要写一个整合上述过程的函数，并使用梯度下降方法对 w 进行优化。具体代码如下：


import os
import pandas as pd
import choose
import numpy as np
import math


def choose_strategy(df, choosename, name, rank, value, m, n, w):
    # 添加技术指标
    try:
        for i in range(1, 16):
            df[f'{i}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-i) / df['收盘']) - 1
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
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nlargest(rank, '昨日资金波动')).reset_index(drop=True)
        if ('股票' in name):
            if ('分钟' not in name):
                df = df[(df['真实价格'] >= 4)].copy()  # 过滤低价股
                df = df[(df['开盘收盘幅'] <= 0.08) & (
                    df['开盘收盘幅'] >= -0.01)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 3*w*value/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nlargest(rank, '昨日资金波动')).reset_index(drop=True)

    df = df.sort_values(by='日期')  # 以日期列为索引,避免计算错误
    dates = df['日期'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
    df = df.groupby(['代码'], group_keys=False).apply(
        choose.technology)
    # 去掉噪音数据
    for n in range(1, 9):
        df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]
    m = 0.001  # 设置手续费
    n = 6  # 设置持仓周期
    df, m, n = choose.choose('交易', name, df)
    df.to_csv(f'{name}交易细节.csv', index=False)  # 输出交易细节
    result_df = pd.DataFrame({})
    cash_balance = 1  # 初始资金设置为1元
    daily_cash_balance = {}  # 用于记录每日的资金余额
    result = []
    for date in dates:
        day = dates.index(date)
        days = dates[day-n:day]
        # 取从n天前到当天的数据
        daydf = df.loc[df['日期'].isin(days)]
        daydf = daydf.fillna(1)
        daydf = daydf.sort_values(by='日期')    # 以日期列为索引,避免计算错误
        daydates = daydf['日期'].copy(
        ).drop_duplicates().tolist()  # 获取所有不重复日期
        daily_ret = 0
        m = m/n  # 一天手续费均摊到n天就是
        if daydates:
            for i in range(0, len(daydates)-1):
                ret = daydf[daydf['日期'] == daydates[i]
                            ][f'{i+1}日后当日涨跌（未来函数）'].mean()*(1-m)-1
                daily_ret += ret/n
                cash_balance *= (1 + daily_ret)
                daily_cash_balance[date] = cash_balance
                result.append({'日期': date, f'盘中波动': cash_balance})
            result_df = pd.concat([result_df, pd.DataFrame(result)])
            print(result_df)
    print(f'本回合权重系数w={w},目标收益{cash_balance}')
    return cash_balance


def gradient_descent(df, choosename, name, rank, value, m, n):
    # 梯度下降寻找 w 的最优值
    w = 0.2  # 初始值
    lr = 0.0001  # 学习率
    delta = 0.001  # 步长
    precision = 0.01  # 精度
    last_w = 0
    while True:
        profit = choose_strategy(df, choosename, name, rank, value, m, n, w)
        dprofit = (choose_strategy(df, choosename, name, rank,
                   value, m, n, w+delta) - profit) / delta
        w = w + lr * dprofit
        if abs(w - last_w) < precision:
            break
        else:
            last_w = w
    return w


# names = ['COIN', '股票', '指数', '行业']
names = ['指数', '行业']

updown = '盘中波动'  # 计算当日理论上的盘中每日回撤
# updown = '资产收益'  # 计算每份资金的资产收益率

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file) & ('指标' in file) & ('排名' not in file) & ('细节' not in file) & ('分钟' not in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f'{name}.csv')
                print(name)
                df = pd.read_csv(path)
                w = gradient_descent(df, '交易', '股票', 10, 0.5, 0.005, 15)
                print(f"{name}通过梯度下降，得到的最优权重系数 w 为：{w}")
            except Exception as e:
                print(f"发生bug: {e}")
