import math
import pandas as pd
import os

# name = 'BTC'
name = '指数'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)


df_mean = df.groupby('日期')[f'SMA{20}开盘比值'].mean().reset_index(name='均值')
df_mean['策略'] = df_mean['均值'].apply(lambda x: '震荡策略' if x >= 1 else '超跌策略')
df_merged = pd.merge(df, df_mean[['日期', '策略']], on='日期', how='left')
df = df_merged[df_merged['策略'] == '震荡策略'].copy()
df['score'] = 0


def midfilter(df, col_name, start, end):  # 参数col_names表示列名，参数start表示去掉前百分之多少，参数end表示去掉后百分之多少
    df = df[df[col_name] >= df[col_name].quantile(start) & (
        df[col_name] < df[col_name].quantile(end))]
    return df


def topfilter(df, col_name, start, end):  # 参数col_names表示列名，参数start表示保留前百分之多少，参数end表示保留后百分之多少
    df = df[df[col_name] >= df[col_name].quantile(start) | (
        df[col_name] < df[col_name].quantile(end))]
    return df


# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

code_count = len(df['代码'].drop_duplicates())
print("标的数量", code_count)

if 'btc' in name.lower():
    # 成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 200000].copy()
    df['score'] += df.groupby('日期')[f'SMA{20}开盘比值'].apply(
        lambda x: 1 if x >= df[f'SMA{20}开盘比值'].quantile(0.95) else 0)  # 确认长期趋势上涨
    df['score'] += df.groupby('日期')['昨日振幅'].apply(
        lambda x: 1 if x >= df['昨日振幅'].quantile(0.95) else 0)  # 振幅较大，趋势明显
    for n in range(6, 11):
        df['score'] += df[f'SMA{n}开盘比值'].apply(
            lambda x: 1 if x >= 1 else 0)  # 确认短期趋势下跌
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.01].copy()
if '指数' in name.lower():
    # 成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 20000000].copy()
    df['score'] += df.groupby('日期')[f'SMA{20}开盘比值'].apply(
        lambda x: 1 if x >= df[f'SMA{20}开盘比值'].quantile(0.95) else 0)  # 确认长期趋势上涨
    df['score'] += df.groupby('日期')['昨日振幅'].apply(
        lambda x: 1 if x >= df['昨日振幅'].quantile(0.95) else 0)  # 振幅较大，趋势明显
    for n in range(6, 11):
        df['score'] += df[f'SMA{n}开盘比值'].apply(
            lambda x: 1 if x >= 1 else 0)  # 确认短期趋势下跌


if 'coin' in name.lower():
    # 昨日成交额过滤劣质股票
    df = df[df[f'昨日成交额'] >= 1000000].copy()
    # 牛市过滤
    df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
    df['score'] += df.groupby('日期')['昨日振幅'].apply(
        lambda x: 1 if x >= df['昨日振幅'].quantile(0.95) else 0)  # 振幅较大，趋势明显
    df['score'] += df.groupby('日期')[f'开盘'].apply(
        lambda x: 1 if x >= df[f'开盘'].quantile(0.95) else 0)  # 确认价格较低
    # 开盘价过滤高滑点股票
    df = df[df[f'开盘'] >= 0.00000500].copy()


if 'stock' in name.lower():
    # 价格过滤劣质股票
    df = df[(df['真实价格'] >= 4)].copy()
    # 牛市过滤
    df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
    df['score'] += df.groupby('日期')['昨日振幅'].apply(
        lambda x: 1 if x >= df['昨日振幅'].quantile(0.95) else 0)  # 振幅较大，趋势明显
    df['score'] += df.groupby('日期')[f'开盘'].apply(
        lambda x: 1 if x >= df[f'昨日成交额'].quantile(0.95) else 0)  # 确认价格较低
    # 开盘收盘幅过滤无法买入股票
    df = df[
        (df['开盘收盘幅'] <= 8)
        &
        (df['开盘收盘幅'] >= 0)
    ].copy()


# 每天选择分值较高的10个股票，总分值大于5
df = df.groupby(['日期']).apply(
    lambda x: x.nlargest(10, 'score')).reset_index(drop=True)
df = df.groupby(['日期']).filter(lambda x: x['score'].sum() > 5)
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
    n = 20  # 设置持仓周期
    m = 0.0005  # 设置手续费
if '指数' in name.lower():
    n = 20  # 设置持仓周期
    m = 0.0005  # 设置手续费

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
