import math
import pandas as pd
import os

# 设置参数
# name = 'BTC'
# name = 'COIN'
# name = '上证'
name = '深证'

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

if 'btc' in name.lower():
    # 排除掉资金结算前后的持仓
    df = df[df[f'昨日成交额'] >= 10000].copy()  # 成交额过滤劣质股票
    df = df[df[f'开盘'] >= 0.01].copy()  # 开盘价过滤高滑点股票
    # 正向
    df = df[(df['昨日资金贡献_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    df = df[(df['昨日资金波动_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    for n in range(2, 20):  # 对短期趋势上涨进行打分
        df = df[(df[f'过去{n}日总涨跌'] >= 0.1)].copy()
        df = df[(df[f'过去{n}日资金贡献_rank'] <= 0.3)].copy()
        df = df[(df[f'过去{n}日总成交额_rank'] >= 0.7)].copy()
    df['资金结算'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df[df['资金结算'].apply(lambda x: not ((x.hour in [7, 15, 23]) and (x.minute > 40)))]
    print(len(df))
if 'coin' in name.lower():
    df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
    df = df[df[f'开盘'] >= 0.00000500].copy()  # 开盘价过滤高滑点股票
    # 正向
    df = df[(df['昨日资金贡献_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    df = df[(df['昨日资金波动_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    for n in range(2, 20):  # 对短期趋势上涨进行打分
        df = df[(df[f'过去{n}日总涨跌'] >= 0.1)].copy()
        df = df[(df[f'过去{n}日资金贡献_rank'] <= 0.3)].copy()
        df = df[(df[f'过去{n}日总成交额_rank'] >= 0.7)].copy()
    print(len(df))
if '证' in name.lower():
    df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
    df = df[(df['开盘收盘幅'] <= 1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    # 正向
    df = df[(df['昨日资金贡献_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    df = df[(df['昨日资金波动_rank'] <= 0.1)].copy()  # 开盘收盘幅过滤涨停无法买入股票
    for n in range(2, 20):  # 对短期趋势上涨进行打分
        df = df[(df[f'过去{n}日总涨跌'] >= 0.1)].copy()
        df = df[(df[f'过去{n}日资金贡献_rank'] <= 0.3)].copy()
        df = df[(df[f'过去{n}日总成交额_rank'] >= 0.7)].copy()
    print(len(df))

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 假设开始时有1元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 1
# 用于记录每日的资金余额
daily_cash_balance = {}

if '证' in name.lower():
    n = 18  # 设置持仓周期
    m = 0.005  # 设置手续费
if 'coin' in name.lower():
    n = 18  # 设置持仓周期
    m = 0.005  # 设置手续费
    # df['资金结算'] = pd.to_datetime(df['timestamp'], unit='s')
    # df = df[df['资金结算'].apply(lambda x: not ((x.hour in [7, 15, 23]) and (x.minute > 40)))]
if 'btc' in name.lower():
    n = 18  # 设置持仓周期
    m = 0.0005  # 设置手续费
    df['资金结算'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df[df['资金结算'].apply(lambda x: not (
        (x.hour in [7, 15, 23]) and (x.minute > 40)))]

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
df_strategy_and_return = pd.merge(df_daily_return, df_cash_balance, on='日期')
# 输出每日执行策略和净资产收益率到csv文件
df_strategy_and_return.to_csv(f'{name}每日策略和资产状况.csv', index=False)
