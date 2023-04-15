import pandas as pd

name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

# 标的站在EMA121以上达到某个值的多说明牛市形成，站在EMA121以下达到某个值的多说明熊市形成，分布执行各自的策略

# 所有标的超跌阈值0.5以下的判断熊市中黄金坑形成，执行单独的策略

# df = df[df['EMA121开盘比值'] <= 0.5].copy()

# # 每日选COIN
# n_coin = 500
# df = df.groupby('日期').apply(lambda x: x.nlargest(
#     n_coin, 'EMA9开盘动能4')).reset_index(drop=True)
# n_coin = 10
# df = df.groupby('日期').apply(lambda x: x.nsmallest(
#     n_coin , '开盘')).reset_index(drop=True)
# n_coin = 5
# df = df.groupby('日期').apply(lambda x: x.nsmallest(
#     n_coin, 'EMA121开盘比值')).reset_index(drop=True)
# n_stock = 5
# df = df.groupby('日期').apply(lambda x: x.nlargest(
#     n_stock, '开盘开盘幅')).reset_index(drop=True)


# # 每日选STOCK
# n_stock = 500
# df = df.groupby('日期').apply(lambda x: x.nlargest(
#     n_stock, 'EMA9开盘动能4')).reset_index(drop=True)
n_stock = 100
df = df.groupby('日期').apply(lambda x: x.nsmallest(
    n_stock, '开盘')).reset_index(drop=True)
# n_stock = 5
# df = df.groupby('日期').apply(lambda x: x.nsmallest(
#     n_stock, 'EMA121开盘比值')).reset_index(drop=True)
n_stock = 5
df = df.groupby('日期').apply(lambda x: x.nlargest(
    n_stock, '开盘开盘幅')).reset_index(drop=True)
df = df[
    (df['开盘收盘幅'] <= 8)
    &
    (df['开盘收盘幅'] >= 0)
]

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 计算每日收益率=100*(100+FJ2-2)/100
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

# 假设开始时有10000元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 10000

# 用于记录每日的资金余额
daily_cash_balance = {}

n = 1
# 设置持仓周期
m = 0.01
# 设置手续费

for date, group in df.groupby('日期'):
    # 如果当日没有入选标的，则单日收益率为0
    if group.empty:
        daily_return = 0
    else:
        # 计算单日收益率(平均法)
        group['daily_return'] = group[f'{n}日后总涨跌幅（未来函数）'].mean()/100*(1-m)

        # 计算平均收益率
        daily_return = group['daily_return'].mean()

    # 更新资金余额并记录每日资金余额
    cash_balance *= (1 + daily_return)
    daily_cash_balance[date] = cash_balance

    # 记录每日净资产收益率
    df_daily_return = pd.concat(
        [df_daily_return, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])

# 输出每日净资产收益率到csv文件
df_daily_return.to_csv(f'{name}每日净资产收益率.csv', index=False)

# 将每日资金余额转换为DataFrame
df_cash_balance = pd.DataFrame(
    {'日期': list(daily_cash_balance.keys()), '资金余额': list(daily_cash_balance.values())})

# 将每日资金余额输出到csv文件
df_cash_balance.to_csv(f'{name}每日资金余额.csv', index=False)
