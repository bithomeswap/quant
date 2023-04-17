import pandas as pd

name = 'STOCK_20140101_20170101'
# name = 'COIN'
# name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

df = df[df['EMA121开盘比值'] <= 0.8].copy()

n_stock = 100
df = df.groupby('日期').apply(lambda x: x.nsmallest(
    n_stock, '开盘')).reset_index(drop=True)
n_stock = 5
df = df.groupby('日期').apply(lambda x: x.nsmallest(
    n_stock, 'EMA121开盘比值')).reset_index(drop=True)
df = df[
    (df['开盘收盘幅'] <= 8)
    &
    (df['开盘收盘幅'] >= 0)
]

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 假设开始时有10000元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 10000
# 用于记录每日的资金余额
daily_cash_balance = {}
n = 1
# 设置持仓周期
m = 0
# 设置手续费

df_strategy = pd.DataFrame(columns=['日期', '执行策略'])
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

cash_balance_list = []
# 记录每个交易日是否执行了策略，并输出到csv文件中
for date, group in df.groupby('日期'):
    # 如果当日没有入选标的，则单日收益率为0
    if group.empty:
        daily_return = 0
    else:
        daily_return = (group[f'{n}日后总涨跌幅（未来函数）'] +
                        100).mean()*(1-m)/100-1  # 计算平均收益率
    df_daily_return = pd.concat(
        [df_daily_return, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])
    # 更新资金余额并记录每日资金余额
    cash_balance *= (1 + daily_return)
    daily_cash_balance[date] = cash_balance
    cash_balance_list.append(cash_balance)  # 添加每日资金余额到列表中
df_cash_balance = pd.DataFrame(
    {'日期': list(daily_cash_balance.keys()), '资金余额': list(daily_cash_balance.values())})
df_strategy_and_return = pd.merge(df_daily_return, df_cash_balance, on='日期')
# 输出每日执行策略和净资产收益率到csv文件
df_strategy_and_return.to_csv(f'{name}每日策略和资产状况.csv', index=False)
