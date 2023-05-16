import math
import pandas as pd
import os
names = ['000', '001', '002', '600', '601', '603', '605', '指数', 'ETF', 'COIN']

# updown = '盘中波动'  # 计算当日理论上的盘中每日回撤
updown = '资产收益'  # 计算每份资金的资产收益率


def get_indicators(df):  # 定义计算技术指标的函数
    try:
        # 删除最高价和最低价为负值的数据
        df.drop(df[(df['最高'] < 0) | (df['最低'] < 0)].index, inplace=True)
        df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
        for n in range(1, 20):
            df[f'{n}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-n) / df['收盘']) - 1
            df[f'{n}日后当日涨跌（未来函数）'] = df['涨跌幅'].copy().shift(-n)+1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if filename in file:
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f'{name}.csv')
                df = pd.read_csv(path)
                df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
                dates = df['日期'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
                # 按照“代码”列进行分组并计算技术指标
                df = df.groupby('代码', group_keys=False).apply(get_indicators)
                m = 0.001  # 设置手续费
                n = 6  # 设置持仓周期
                if ('etf' in name.lower()):  # 绝大部分基金靠收管理费赚钱，并不靠净值分红赚钱，分红赚的不如接盘私募多，所以这种衍生品比较弱势
                    if ('分钟' not in name.lower()):
                        df = df[df[f'真实价格'] >= 0.5].copy()  # 真实价格过滤劣质股票
                        # 开盘收盘幅过滤涨停无法买入股票
                        df = df[(df['开盘收盘幅'] <= 0.08)].copy()
                        df = df[(df['昨日涨跌_rank'] >= 0.5)].copy()
                        df = df[(df[f'过去{40}日总涨跌_rank'] >= 0.9)].copy()
                        for n in range(2, 4):
                            df = df[(df[f'过去{n}日总涨跌'] <= 1)].copy()
                        m = 0.005  # 设置手续费
                        n = 4  # 设置持仓周期
                    print(len(df), name)
                if ('coin' in name.lower()):
                    if ('分钟' not in name.lower()):
                        df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                        df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
                        df = df[(df['开盘_rank'] >= 0.5)].copy()  # 真实价格过滤劣质股票
                        df = df[(df['昨日资金波动_rank'] <= 0.15)].copy()
                        df = df[(df['昨日资金贡献_rank'] <= 0.15)].copy()
                        # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
                        # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
                        m = 0.003  # 设置手续费
                        n = 6  # 设置持仓周期
                    if ('分钟' in name.lower()):
                        df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                        for n in (2, 9):
                            df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                            df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                        m = 0.0000  # 设置手续费
                        n = 6  # 设置持仓周期
                    print(len(df), name)
                if ('证' in name.lower()) or ('test' in name.lower()):
                    if ('分钟' not in name.lower()):
                        df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
                        # 开盘收盘幅过滤涨停无法买入股票
                        df = df[(df['开盘收盘幅'] <= 0.01)].copy()
                        df = df[(df['真实价格_rank'] <= 0.8) & (
                            df['真实价格_rank'] >= 0.2)].copy()  # 真实价格过滤劣质股票
                        df = df[(df['昨日资金波动_rank'] <= 0.05)].copy()
                        df = df[(df['昨日资金贡献_rank'] <= 0.05)].copy()
                        # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
                        # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
                        m = 0.005  # 设置手续费
                        n = 15  # 设置持仓周期
                    if ('分钟' in name.lower()):
                        df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
                        for n in (2, 9):
                            df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                            df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                        m = 0.0000  # 设置手续费
                        n = 15  # 设置持仓周期
                    print(len(df), name)
                df.to_csv(f'{name}交易细节.csv', index=False)  # 输出交易细节
                if updown == '盘中波动':
                    result_df = pd.DataFrame({})
                    cash_balance = 1  # 初始资金设置为1元
                    daily_cash_balance = {}  # 用于记录每日的资金余额
                    result = []
                    for date in dates:
                        day = dates.index(date)
                        days = dates[day-n:day]
                        # 取从n天前到当天的数据
                        daydf = df.loc[df['日期'].isin(days)]
                        daydf = daydf.fillna(1)
                        daydf = daydf.sort_values(by='日期')    # 以日期列为索引,避免计算错误
                        daydates = daydf['日期'].copy(
                        ).drop_duplicates().tolist()  # 获取所有不重复日期
                        daily_ret = 0
                        m = m/n  # 一天手续费均摊到n天就是
                        if daydates:
                            for i in range(0, len(daydates)-1):
                                ret = daydf[daydf['日期'] == daydates[i]
                                            ][f'{i+1}日后当日涨跌（未来函数）'].mean()*(1-m)-1
                                daily_ret += ret/n
                        cash_balance *= (1 + daily_ret)
                        daily_cash_balance[date] = cash_balance
                        result.append({'日期': date, f'盘中波动': cash_balance})
                    result_df = pd.concat([result_df, pd.DataFrame(result)])
                    print(result_df)
                    # 新建涨跌分布文件夹在上级菜单下，并保存结果
                    parent_path = os.path.abspath('.')
                    dir_name = '资产变动'
                    path = os.path.join(parent_path, dir_name)
                    if not os.path.exists(path):
                        os.makedirs(path)
                    result_df.to_csv(
                        f'{path}/{name}_{updown}资金波动.csv', index=False)
                if updown == '资产收益':
                    result_df = pd.DataFrame({})
                    for i in range(1, n+1):
                        # 持有n天则掉仓周期为n，实际上资金实盘当中是单独留一份备用金补给亏的多的日期以及资金周转
                        days = dates[i::n]
                        daydf = df.loc[df['日期'].isin(days)]
                        # daydf.to_csv(f'{name}_{i}份交易细节.csv', index=False)  # 输出每份资金的交易细节
                        cash_balance = 1  # 初始资金设置为1元
                        daily_cash_balance = {}  # 用于记录每日的资金余额
                        result = []
                        # 每份资金的收益率
                        for date, group in daydf.groupby('日期'):
                            if group.empty:  # 如果当日没有入选标的，则收益率为0
                                daily_return = 0
                            else:
                                daily_return = (
                                    group[f'{n}日后总涨跌幅（未来函数）'].mean() + 1)*(1-m)-1  # 计算平均收益率
                            # 更新资金余额并记录每日资金余额
                            cash_balance *= (1 + daily_return)
                            daily_cash_balance[date] = cash_balance
                            result.append(
                                {'日期': date, f'第{i}份资金收益率': cash_balance})
                        result_df = pd.concat(
                            [result_df, pd.DataFrame(result)])
                    # 新建涨跌分布文件夹在上级菜单下，并保存结果
                    parent_path = os.path.abspath('.')
                    dir_name = '资产变动'
                    path = os.path.join(parent_path, dir_name)
                    if not os.path.exists(path):
                        os.makedirs(path)
                    result_df.to_csv(
                        f'{path}/{name}_{updown}理论收益.csv', index=False)
                    print('任务已经完成！')
            except Exception as e:
                print(f"发生bug: {e}")
