import math
import pandas as pd
import os

name = 'COIN'
# name = 'STOCK'
# name = 'BTC'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

code_count = len(df['代码'].drop_duplicates())
print("标的数量", code_count)

for n in range(1, 5):  # 去掉n日后总涨跌幅大于百分之三百的噪音数据
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]

# 多空区分备份
df_mean = df.groupby('日期')[f'SMA{20}开盘比值'].mean().reset_index(name='均值')
df_mean['策略'] = df_mean['均值'].apply(lambda x: '多头行情' if x >= 1 else '空头行情')
df_merged = pd.merge(df, df_mean[['日期', '策略']], on='日期', how='left')
# df = df_merged[df_merged['策略'] == '空头策略'].copy()
# df = df_merged[df_merged['策略'] == '多头策略'].copy()
df['score'] = 0

# 空头备份
if 'btc' in name.lower():
    # 成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 20000].copy()
    # 60日相对超涨
    n_stock = math.ceil(code_count/50)
    df = df.groupby('日期').apply(lambda x: x.nlargest(
        n_stock, f'SMA{60}开盘比值')).reset_index(drop=True)
    # 振幅较大，趋势明显
    n_stock = math.ceil(code_count/100)
    df = df.groupby('日期').apply(lambda x: x.nlargest(
        n_stock, '昨日振幅')).reset_index(drop=True)
    # 确认短期趋势下跌
    for n in range(6, 11):
        df = df[df[f'SMA{n}开盘比值'] <= 1].copy()
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.01].copy()
    print(len(df))
# 多头备份
if 'btc' in name.lower():
    # 成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 200000].copy()
    # 60日相对超跌
    n_stock = math.ceil(code_count/50)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, f'SMA{60}开盘比值')).reset_index(drop=True)
    # 振幅较大，趋势明显
    n_stock = math.ceil(code_count/100)
    df = df.groupby('日期').apply(lambda x: x.nlargest(
        n_stock, '昨日振幅')).reset_index(drop=True)
    # 确认短期趋势
    for n in range(6, 11):
        df = df[df[f'SMA{n}开盘比值'] >= 1].copy()
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.01].copy()
    print(len(df))
if 'coin' in name.lower():
    # 昨日成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 1000000].copy()
    # 牛市过滤
    df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
    n_stock = math.ceil(code_count/5)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, f'SMA{100}开盘比值')).reset_index(drop=True)
    n_stock = math.ceil(code_count/10)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, '昨日振幅')).reset_index(drop=True)
    n_stock = math.ceil(code_count/100)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, '开盘')).reset_index(drop=True)
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.00000500].copy()
    print(len(df))
if 'stock' in name.lower():
    # 价格过滤劣质股票
    df = df[(df['真实价格'] >= 4)].copy()
    # 牛市过滤
    df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
    n_stock = math.ceil(code_count/5)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, f'SMA{100}开盘比值')).reset_index(drop=True)
    n_stock = math.ceil(code_count/10)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, '昨日振幅')).reset_index(drop=True)
    n_stock = math.ceil(code_count/100)
    df = df.groupby('日期').apply(lambda x: x.nsmallest(
        n_stock, '昨日成交额')).reset_index(drop=True)
    # 开盘收盘幅过滤无法买入股票
    df = df[
        (df['开盘收盘幅'] <= 0.2)
        &
        (df['开盘收盘幅'] >= 0)
    ].copy()
    print(len(df))

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 假设开始时有1元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 1
# 用于记录每日的资金余额
daily_cash_balance = {}

if 'stock' in name.lower():
    n = 6  # 设置持仓周期
    m = 0.005  # 设置手续费
if 'coin' in name.lower():
    n = 6  # 设置持仓周期
    m = 0.005  # 设置手续费
if 'btc' in name.lower():
    n = 15  # 设置持仓周期
    m = -0.0005  # 设置手续费

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

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
