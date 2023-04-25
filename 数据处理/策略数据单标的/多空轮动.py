import math
import pandas as pd
import os

name = 'BTC'
# name = '上证指数'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 下降通道做多
df = df[df['SMA120开盘比值'] <= 0.995].copy()
for n in range(1, 10):  # 计算未来n日涨跌幅
    df = df[df[f'{n*10}日最高开盘价比值'] <= 1-n*0.001].copy()
# 上升通道做空（手续费设置为负的情况下，亏损越多越好）
# df = df[df['SMA120开盘比值'] >= 1.02].copy()
# for n in range(1, 10):  # 计算未来n日涨跌幅
#     df = df[df[f'{n*10}日最低开盘价比值'] >= 1+n*0.001].copy()

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 假设开始时有1元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 1
# 用于记录每日的资金余额
daily_cash_balance = {}
n = 9
# 设置持仓周期
m = 0.0005
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
        daily_return = ((group[f'{n}日后总涨跌幅（未来函数）'] +
                        1).mean()*(1-m)/-1)/n  # 计算平均收益率
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
