import math
import pandas as pd
import os

name = "BTC"
# name = "指数"
# name = 'COIN'
# name = 'STOCK'

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上三级目录的路径
dir_path = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
file_path = os.path.join(dir_path, f'{name}指标.csv')
df = pd.read_csv(file_path)

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

code_count = len(df['代码'].drop_duplicates())

if 'btc' in name.lower():
    # 计算每个交易日成分股的'SMA120开盘比值'均值
    df_mean = df.groupby('日期')[f'SMA{60}开盘比值', f'SMA{100}开盘比值'].mean()
    # df_mean['均值'] = df_mean.drop('日期', axis=1).mean(axis=1)
    print(df_mean)

    # 根据规则对每个交易日进行标注,一般这个值大于等于1就行，
    # 只是震荡行情本身也是有区别的，这个阈值比较难确定
    df_mean['策略'] = df_mean['均值'].apply(
        lambda x: '震荡策略' if x >= 1 else '超跌策略')
# if '指数' in name.lower():
#     # 计算每个交易日成分股的'SMA120开盘比值'均值
#     df_mean = df.groupby('日期')[f'SMA{20}开盘比值'].mean().reset_index(name='均值')
#     # 根据规则对每个交易日进行标注,一般这个值大于等于1就行，
#     # 只是震荡行情本身也是有区别的，这个阈值比较难确定
#     df_mean['策略'] = df_mean['均值'].apply(
#         lambda x: '震荡策略' if x >= 1 else '超跌策略')
# if 'coin' in name.lower():
#     # 计算每个交易日成分股的'SMA120开盘比值'均值
#     df_mean = df.groupby('日期')[f'SMA{30}开盘比值'].mean().reset_index(name='均值')
#     # 根据规则对每个交易日进行标注,一般这个值大于等于1就行，
#     # 只是震荡行情本身也是有区别的，这个阈值比较难确定
#     df_mean['策略'] = df_mean['均值'].apply(
#         lambda x: '震荡策略' if x >= 1.002 else '超跌策略')
# if 'stock' in name.lower():
#     # 计算每个交易日成分股的'SMA120开盘比值'均值
#     df_mean = df.groupby('日期')[f'SMA{30}开盘比值'].mean().reset_index(name='均值')
#     # 根据规则对每个交易日进行标注,一般这个值大于等于1就行，
#     # 只是震荡行情本身也是有区别的，这个阈值比较难确定
#     df_mean['策略'] = df_mean['均值'].apply(
#         lambda x: '震荡策略' if x >= 0.92 else '超跌策略')
# # 输出到csv文件
# df_mean.to_csv(f'{name}牛熊特征.csv', index=False)


# def oscillating_strategy(df):  # 实现震荡策略
#     if 'btc' in name.lower():
#         # 成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 200000].copy()
#         # 60日相对超跌
#         n_stock = math.ceil(code_count/50)
#         df = df.nsmallest(n_stock, f'SMA{60}开盘比值')
#         n_stock = math.ceil(code_count/50)
#         df = df.nsmallest(n_stock, '昨日振幅')
#         # 确认短期趋势
#         for n in range(6, 11):
#             df = df[df[f'SMA{n}开盘比值'] >= 1].copy()
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.01].copy()
#         # print(len(df))
#     if '指数' in name.lower():
#         # 成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 20000000].copy()
#         # 60日相对超跌
#         n_stock = math.ceil(code_count/5)
#         df = df.nsmallest(n_stock, f'SMA{60}开盘比值')
#         n_stock = math.ceil(code_count/10)
#         df = df.nsmallest(n_stock, '昨日振幅')
#         # 确认短期趋势
#         for n in range(6, 11):
#             df = df[df[f'SMA{n}开盘比值'] >= 1].copy()
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.01].copy()
#         print(len(df))
#     if 'coin' in name.lower():
#         # 昨日成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 1000000].copy()
#         # 牛市过滤
#         df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
#         n_stock = math.ceil(code_count/5)
#         df = df.nsmallest(n_stock, f'SMA{100}开盘比值')
#         n_stock = math.ceil(code_count/10)
#         df = df.nsmallest(n_stock, '昨日振幅')
#         n_stock = math.ceil(code_count/100)
#         df = df.nsmallest(n_stock, '开盘')
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.00000500].copy()
#     if 'stock' in name.lower():
#         # 价格过滤劣质股票
#         df = df[(df['真实价格'] >= 4)].copy()
#         # 牛市过滤
#         df = df[df[f'SMA{20}开盘比值'] >= 1].copy()
#         n_stock = math.ceil(code_count/5)
#         df = df.nsmallest(n_stock, f'SMA{100}开盘比值')
#         n_stock = math.ceil(code_count/10)
#         df = df.nsmallest(n_stock, '昨日振幅')
#         n_stock = math.ceil(code_count/100)
#         df = df.nsmallest(n_stock, '昨日成交额')
#         df = df[
#             (df['开盘收盘幅'] <= 0.2)
#             &
#             (df['开盘收盘幅'] >= 0)
#         ].copy()
#     return df


# def oversold_strategy(df):  # 实现超跌策略
#     if 'btc' in name.lower():
#         # 成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 20000].copy()
#         # 60日相对超涨
#         n_stock = math.ceil(code_count/50)
#         df = df.nlargest(n_stock, f'SMA{60}开盘比值')
#         n_stock = math.ceil(code_count/50)
#         df = df.nlargest(n_stock, '昨日振幅')
#         # 确认短期趋势下跌
#         for n in range(6, 11):
#             df = df[df[f'SMA{n}开盘比值'] <= 1].copy()
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.01].copy()
#     if '指数' in name.lower():
#         # 成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 20000000].copy()
#         # 60日相对超涨
#         n_stock = math.ceil(code_count/50)
#         df = df.nlargest(n_stock, f'SMA{60}开盘比值')
#         n_stock = math.ceil(code_count/50)
#         df = df.nlargest(n_stock, '昨日振幅')
#         # 确认短期趋势下跌
#         for n in range(6, 11):
#             df = df[df[f'SMA{n}开盘比值'] <= 1].copy()
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.01].copy()
#     if 'coin' in name.lower():
#         # 成交额过滤劣质股票
#         df = df[df[f'昨日成交额'] >= 1000000].copy()
#         # # # 熊市过滤
#         df = df[df[f'SMA{60}开盘比值'] <= 0.5].copy()
#         df = df[df[f'SMA{10}开盘比值'] >= 1].copy()
#         # 开盘价过滤高滑点股票
#         df = df[df[f'开盘'] >= 0.00000500].copy()
#     if 'stock' in name.lower():
#         # 价格过滤劣质股票
#         df = df[(df['真实价格'] >= 4)].copy()
#         # 熊市过滤
#         df = df[df[f'SMA{60}开盘比值'] <= 0.5].copy()
#         df = df[df[f'SMA{10}开盘比值'] >= 1].copy()
#         df = df[
#             (df['开盘收盘幅'] <= 0.2)
#             &
#             (df['开盘收盘幅'] >= 0)
#         ].copy()
#     return df


# selectedzhendang = pd.DataFrame(columns=[])
# selectedchaodie = pd.DataFrame(columns=[])
# # 记录每个交易日是否执行了策略，并输出到csv文件中
# for date, group in df.groupby('日期'):
#     # 如果当日没有入选标的，则单日收益率为0
#     if group.empty:
#         daily_return = 0
#     else:
#         # 过滤掉空数据
#         if df_mean[df_mean['日期'] == date]['策略'].size > 0:
#             # 根据标注的策略执行相应的策略
#             if df_mean[df_mean['日期'] == date]['策略'].iloc[0] == '震荡策略':
#                 selected_stocks = oscillating_strategy(group)
#                 selectedzhendang = pd.concat(
#                     [selectedzhendang, selected_stocks])
#             else:
#                 selected_stocks = oversold_strategy(group)
#                 selectedchaodie = pd.concat([selectedchaodie, selected_stocks])

# # selectedzhendang.to_csv(f'{name}标的震荡策略详情.csv', index=False)
# # selectedchaodie.to_csv(f'{name}标的超跌策略详情.csv', index=False)

# cash_balance_zhendang = 1  # 假设开始时有1元资金（震荡策略）
# cash_balance_chaodie = 1  # 假设开始时有1元资金（超跌策略）
# daily_cash_balance_zhendang = pd.DataFrame(
#     columns=['日期', '资金余额'])  # 用于记录每日的资金余额（震荡策略）
# daily_cash_balance_chaodie = pd.DataFrame(
#     columns=['日期', '资金余额'])  # 用于记录每日的资金余额（超跌策略）


# df_daily_return_zhendang = pd.DataFrame(columns=['日期', '收益率'])
# # 记录每个交易日是否执行了策略，并输出到csv文件中
# for date, group in selectedzhendang.groupby('日期'):
#     if 'btc' in name.lower():
#         n = 20  # 设置持仓周期
#         m = 0.0005  # 设置手续费
#     if '指数' in name.lower():
#         n = 20  # 设置持仓周期
#         m = 0.0005  # 设置手续费
#     if 'coin' in name.lower():
#         n = 6  # 设置持仓周期
#         m = 0.005  # 设置手续费
#     if 'stock' in name.lower():
#         n = 6  # 设置持仓周期
#         m = 0.005  # 设置手续费
#     # 如果当日没有入选标的，则单日收益率为0
#     if group.empty:
#         daily_return = 0
#     else:
#         daily_return = ((group[f'{n}日后总涨跌幅（未来函数）'] +
#                         1).mean()*(1-m)-1)/n  # 计算平均收益率
#     # 更新资金余额并记录每日资金余额
#     df_daily_return_zhendang = pd.concat(
#         [df_daily_return_zhendang, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])
#     cash_balance_zhendang *= (1 + daily_return)
#     daily_cash_balance_zhendang = pd.concat(
#         [daily_cash_balance_zhendang, pd.DataFrame({'日期': [date], '资金余额': [cash_balance_zhendang]})])

# df_daily_return_chaodie = pd.DataFrame(columns=['日期', '收益率'])
# # 记录每个交易日是否执行了策略，并输出到csv文件中
# for date, group in selectedchaodie.groupby('日期'):
#     if 'btc' in name.lower():
#         n = 20  # 设置持仓周期
#         m = -0.005  # 设置手续费
#     if '指数' in name.lower():
#         n = 20  # 设置持仓周期
#         m = -0.0005  # 设置手续费
#     if 'coin' in name.lower():
#         n = 6  # 设置持仓周期
#         m = 0.01  # 设置手续费
#     if 'stock' in name.lower():
#         n = 9  # 设置持仓周期
#         m = 0.005  # 设置手续费
#     # 如果当日没有入选标的，则单日收益率为0
#     if group.empty:
#         daily_return = 0
#     else:
#         daily_return = ((group[f'{n}日后总涨跌幅（未来函数）'] +
#                         1).mean()*(1-m)-1)/n  # 计算平均收益率
#     # 更新资金余额并记录每日资金余额
#     df_daily_return_chaodie = pd.concat(
#         [df_daily_return_chaodie, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])
#     cash_balance_chaodie *= (1 + daily_return)
#     daily_cash_balance_chaodie = pd.concat(
#         [daily_cash_balance_chaodie, pd.DataFrame({'日期': [date], '资金余额': [cash_balance_chaodie]})])


# daily_cash_balance_zhendangpd = pd.merge(
#     df_daily_return_zhendang, daily_cash_balance_zhendang, on='日期')
# daily_cash_balance_chaodie = pd.merge(
#     df_daily_return_chaodie, daily_cash_balance_chaodie, on='日期')

# daily_cash_balance_zhendangpd.to_csv(f'{name}标的震荡策略资产状况.csv', index_label='日期')
# daily_cash_balance_chaodie.to_csv(f'{name}标的超跌策略资产状况.csv', index_label='日期')
