import math
import pandas as pd
import os

name = 'COIN'
# name = 'STOCK'
# name = 'COIN止损'
# name = 'STOCK止损'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(
    os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 获取自制成分股指数
code_count = len(df['代码'].drop_duplicates())
n_stock = code_count // 10
codes = df.groupby('代码')['成交额'].mean().nlargest(n_stock).index.tolist()
df = df[df['代码'].isin(codes)]
print("自制成分股指数为：", codes)

# 计算每个交易日的'SMA121开盘比值'均值
df_mean = df.groupby('日期')['SMA121开盘比值'].mean().reset_index(name='均值')
# 根据规则对每个交易日进行标注
df_mean['策略'] = df_mean['均值'].apply(lambda x: '震荡策略' if x >= 1 else '超跌策略')
# 输出到csv文件
df_mean.to_csv(f'{name}牛熊特征.csv', index=False)


def oscillating_strategy(df):  # 实现震荡策略
    if 'stock' in name.lower():
        # 然后选取当天'开盘开盘幅'最大的百分之十
        n_top = math.floor(len(df)*0.1)
        df = df.nlargest(n_top, '开盘开盘幅')
    # 再选取当天'160日最高开盘价比值'最低的百分之一
    n_top = math.floor(len(df)*0.01)
    df = df.nsmallest(n_top, '开盘')
    if 'stock' in name.lower():
        df = df[(df['开盘收盘幅'] <= 8) & (df['开盘收盘幅'] >= 0)]
    return df


def oversold_strategy(df):  # 实现超跌策略
    # 先排除SMA121开盘比值在0.8以下的数据
    df = df[df['SMA121开盘比值'] >= 0.8]
    if 'stock' in name.lower():
        # 然后选取当天'开盘开盘幅'最大的百分之十
        n_top = math.floor(len(df)*0.1)
        df = df.nlargest(n_top, '开盘开盘幅')
    # 再选取当天'160日最高开盘价比值'最低的百分之一
    n_top = math.floor(len(df)*0.01)
    df = df.nsmallest(n_top, '160日最高开盘价比值')
    if 'stock' in name.lower():
        df = df[(df['开盘收盘幅'] <= 8) & (df['开盘收盘幅'] >= 0)]
    return df


selectedzhendang = pd.DataFrame(columns=[])
selectedchaodie = pd.DataFrame(columns=[])
# 记录每个交易日是否执行了策略，并输出到csv文件中
for date, group in df.groupby('日期'):
    # 如果当日没有入选标的，则单日收益率为0
    if group.empty:
        daily_return = 0
    else:
        # 根据标注的策略执行相应的策略
        if df_mean[df_mean['日期'] == date]['策略'].iloc[0] == '震荡策略':
            selected_stocks = oscillating_strategy(group)
            selectedzhendang = pd.concat([selectedzhendang, selected_stocks])
        else:
            selected_stocks = oversold_strategy(group)
            selectedchaodie = pd.concat([selectedchaodie, selected_stocks])

selectedzhendang.to_csv(f'{name}标的震荡策略详情.csv', index=False)
selectedchaodie.to_csv(f'{name}标的超跌策略详情.csv', index=False)

cash_balance = 10000  # 假设开始时有10000元资金
daily_cash_balance = {}  # 用于记录每日的资金余额
n = 1  # 设置持仓周期
m = 0  # 设置手续费
df_strategy = pd.DataFrame(columns=['日期', '执行策略'])
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])
cash_balance_list = []
# 记录每个交易日是否执行了策略，并输出到csv文件中
for date, group in selectedzhendang.groupby('日期'):
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
df_strategy_and_return.to_csv(f'{name}标的震荡策略资产状况.csv', index=False)
