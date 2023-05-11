import math
import pandas as pd
import os

# 设置参数
names = ['COIN', '分钟COIN', '深证', '分钟深证', '上证', '分钟上证',]

for name in names:
    # 获取当前.py文件的绝对路径
    file_path = os.path.abspath(__file__)
    # 获取当前.py文件所在目录的路径
    dir_path = os.path.dirname(file_path)
    # 获取当前.py文件所在目录的上两级目录的路径
    dir_path = os.path.dirname(os.path.dirname(dir_path))
    file_path = os.path.join(dir_path, f'{name}指标.csv')
    df = pd.read_csv(file_path)

    code_count = len(df['代码'].drop_duplicates())
    print("标的数量", code_count)

    for n in range(1, 9):  # 去掉n日后总涨跌幅大于百分之三百的噪音数据
        df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]

    if ('coin' in name.lower()) and ('分钟' not in name.lower()):
        # df_mean = df.groupby('日期')[f'昨日资金贡献_rank'].mean().reset_index(name='均值')
        # df_mean['策略'] = df_mean['均值'].apply(lambda x: '多头策略' if x >= 0 else '空头策略')

        # df_quantile = df.groupby('日期')[f'昨日资金贡献_rank'].quantile(q=0.33).reset_index(name='第一三分位数')
        # df_quantile['策略'] = df_quantile['第一三分位数'].apply(lambda x: '多头策略' if x >= 0 else '空头策略')
        # 这里的分位数计算是从小到大的

        # # df_merged = pd.merge(df, df_mean[['日期', '策略']], on='日期', how='left')
        # # df = df_merged[df_merged['策略'] == '多头策略'].copy()

        df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
        df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
        # 正向
        df = df[(df['昨日资金贡献_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        df = df[(df['昨日资金波动_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        for n in range(6, 10):  # 对短期趋势上涨进行打分
            df = df[(df[f'过去{n}日总成交额_rank'] >= 0.8)].copy()
            df = df[(df[f'过去{n}日资金贡献_rank'] <= 0.2)].copy()
        print(len(df), name)
        n = 9  # 设置持仓周期
        m = 0.003  # 设置手续费
    # 缩量下跌的转正时刻
    if ('coin' in name.lower()) and ('分钟' in name.lower()):
        df = df[df[f'昨日成交额'] >= 10000].copy()  # 成交额过滤劣质股票
        df = df[df[f'开盘'] >= 0.01].copy()  # 开盘价过滤高滑点股票
        # 正向
        df = df[(df[f'昨日涨跌'] >= 1)].copy()
        df = df[(df['昨日资金波动_rank'] >= 0.5)].copy()
        for n in range(6, 10):  # 过去几天在下跌
            df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
            df = df[(df[f'过去{n}日资金贡献_rank'] >= 0.8)].copy()
        print(len(df), name)
        n = 9  # 设置持仓周期
        m = 0.0000  # 设置手续费
    if ('证' in name.lower()) and ('分钟' not in name.lower()):
        df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
        df = df[(df['开盘收盘幅'] <= 1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        # 正向
        df = df[(df['昨日资金波动_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        df = df[(df['昨日资金贡献_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        for n in range(6, 10):  # 过去几天在下跌
            df = df[(df[f'过去{n}日总成交额_rank'] >= 0.8)].copy()
            df = df[(df[f'过去{n}日资金贡献_rank'] <= 0.2)].copy()
        print(len(df), name)
        n = 9  # 设置持仓周期
        m = 0.005  # 设置手续费
    if ('证' in name.lower()) and ('分钟' in name.lower()):
        df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
        df = df[(df['开盘收盘幅'] <= 1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
        # 正向
        df = df[(df[f'昨日涨跌'] >= 1)].copy()
        df = df[(df['昨日资金波动_rank'] >= 0.5)].copy()
        for n in range(6, 10):  # 过去几天在下跌
            df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
            df = df[(df[f'过去{n}日资金贡献_rank'] >= 0.8)].copy()
        print(len(df), name)
        n = 9  # 设置持仓周期
        m = 0.0000  # 设置手续费

    # 将交易标的细节输出到一个csv文件
    trading_detail_filename = f'{name}交易标的细节.csv'
    df.to_csv(trading_detail_filename, index=False)

    # 假设开始时有1元资金,实操时每个月还得归集一下资金，以免收益不平均
    cash_balance = 1
    # 用于记录每日的资金余额
    daily_cash_balance = {}

    df_strategy = pd.DataFrame(columns=['日期', '执行策略'])
    df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

    cash_balance_list = []
    # 记录每个交易日是否执行了策略，并输出到csv文件中
    for date, group in df.groupby('日期'):
        # 如果当日没有入选标的，则单日收益率为0
        if group.empty:
            daily_return = 0
        else:
            daily_return = ((group[f'{n}日后总涨跌幅（未来函数）'] +
                            1).mean()*(1-m)-1)/n  # 计算平均收益率
        df_daily_return = pd.concat(
            [df_daily_return, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])
        # 更新资金余额并记录每日资金余额
        cash_balance *= (1 + daily_return)
        daily_cash_balance[date] = cash_balance
        cash_balance_list.append(cash_balance)  # 添加每日资金余额到列表中
    df_cash_balance = pd.DataFrame(
        {'日期': list(daily_cash_balance.keys()), '资金余额': list(daily_cash_balance.values())})
    df_strategy_and_return = pd.merge(
        df_daily_return, df_cash_balance, on='日期')
    # 输出每日执行策略和净资产收益率到csv文件
    # df_strategy_and_return.to_csv(f'/{name}每日策略和资产状况.csv', index=False)
    df_strategy_and_return.to_csv(f'./资产变动/{name}每日策略和资产状况.csv', index=False)
