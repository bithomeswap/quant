import pandas as pd

name = 'COIN'
df = pd.read_csv(f'{name}指标.csv')
df = df.dropna()
# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 10):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

# 四均线过滤COIN0.8这里的阈值也应该是动态的，只是多久进行一次调整的问题
df = df[df['EMA121收盘比值'] <= 0.5].copy()
df = df[df['EMA121开盘比值'] <= 0.5].copy()
df = df[df['EMA121最高比值'] <= 0.5].copy()
df = df[df['EMA121最低比值'] <= 0.5].copy()

# # 四均线过滤STOCK0.8这里的阈值也应该是动态的，只是多久进行一次调整的问题
# df = df[df['EMA121收盘比值'] <= 0.8].copy()
# df = df[df['EMA121开盘比值'] <= 0.8].copy()
# df = df[df['EMA121最高比值'] <= 0.8].copy()
# df = df[df['EMA121最低比值'] <= 0.8].copy()

# COIN动能过滤只要最佳的四只
n_stock = 4

# # STOCk动能过滤只要最佳的四只
# n_stock = 20

df = df.groupby('日期').apply(
    lambda x: x.nlargest(n_stock, 'EMA9收盘动能3')).reset_index(drop=True)

#  过滤COIN(df['开盘'] <= 0.9)(df['开盘幅'] <= 9.9)(df['开盘幅'] >= -0.01)
df_stock_filtered = df[(df['开盘'] <= 0.9)
                       & (df['开盘幅'] <= 9.9)
                       ]

# # 过滤COIN(df['开盘'] <= 31)(df['开盘幅'] <= 9.9)(df['开盘幅'] >= -2)(df['换手率'] <= 3.3)
# df_stock_filtered = df[(df['开盘'] <= 31)
#                        & (df['开盘幅'] <= 9.9)
#                        & (df['开盘幅'] >= -2)
#                        & (df['换手率'] <= 3.3)
#                       ]

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df_stock_filtered.to_csv(trading_detail_filename, index=False)

# 计算每日收益率=100*(100+FJ2-2)/100
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

# 假设开始时有10000元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 10000

# 用于记录每日的资金余额
daily_cash_balance = {}

n = 9
for date, group in df_stock_filtered.groupby('日期'):
    # 如果当日没有入选标的，则单日收益率为0
    if group.empty:
        daily_return = 0
    else:
        # 计算单日收益率(平均法)
        group['daily_return'] = group[f'{n}日后总涨跌幅（未来函数）'].mean()/100-0.01

        # 计算平均收益率
        daily_return = group['daily_return'].mean()

    # 更新资金余额并记录每日资金余额
    cash_balance *= (1 + daily_return)
    daily_cash_balance[date] = cash_balance

    # 记录每日净资产收益率
    df_daily_return = df_daily_return.append(
        {'日期': date, '收益率': daily_return}, ignore_index=True)

# 输出每日净资产收益率到csv文件
df_daily_return.to_csv(f'{name}每日净资产收益率.csv', index=False)

# 将每日资金余额转换为DataFrame
df_cash_balance = pd.DataFrame(
    {'日期': list(daily_cash_balance.keys()), '资金余额': list(daily_cash_balance.values())})

# 将每日资金余额输出到csv文件
df_cash_balance.to_csv(f'{name}每日资金余额.csv', index=False)
