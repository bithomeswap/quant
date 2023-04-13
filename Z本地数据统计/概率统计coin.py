import pandas as pd
import math
name = 'COIN'
df = pd.read_csv(f'{name}指标.csv')
df = df.dropna()
# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]
# 所有行业的超跌阈值0.5
df = df[df['EMA121开盘比值'] <= 0.5].copy()
# 安装日期分组
df = df.groupby('日期')
# 每日选取动能最强的一百分之一的标的
n_stock = math.ceil(len(df) / 100)  # 使用math.ceil函数向上取整
df = df.apply(lambda x: x.nsmallest(
    n_stock, 'EMA121开盘比值')).reset_index(drop=True)

#  COIN价格过滤0.9
df = df[(df['开盘'] <= 0.9)]

# # STOCk价格过滤31，COIN高开低开过滤9.9
# df = df[
#     (df['开盘'] <= 31) &
#     (df['开盘收盘幅'] <= 8)
#     & (df['开盘收盘幅'] >= 0)
# ]

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 计算每日收益率=100*(100+FJ2-2)/100
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

# 假设开始时有10000元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 10000

# 用于记录每日的资金余额
daily_cash_balance = {}

n = 6
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
