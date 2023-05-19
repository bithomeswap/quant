import choose
import pandas as pd
import os
# names = ['COIN','股票','指数','行业','ETF']
names = ['行业']

updown = '盘中波动'  # 计算当日理论上的盘中每日回撤
# updown = '资产收益'  # 计算每份资金的资产收益率

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file) & ('指标' in file) & ('排名' not in file)& ('分钟' not in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f'{name}.csv')
                df = pd.read_csv(path)
                df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
                dates = df['日期'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
                df = df.groupby(['代码'], group_keys=False).apply(
                    choose.technology)
                m = 0.001  # 设置手续费
                n = 6  # 设置持仓周期
                df, m, n = choose.choose('交易', name, df)
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
                        daydates = daydf['日期'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
                        daily_ret = 0
                        m = m/n  # 一天手续费均摊到n天就是
                        if daydates:
                            for i in range(0, len(daydates)-1):
                                ret = daydf[daydf['日期'] == daydates[i]][f'{i+1}日后当日涨跌（未来函数）'].mean()*(1-m)-1
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
                        for date, group in daydf.groupby(['日期']):
                            if group.empty:  # 如果当日没有入选标的，则收益率为0
                                daily_return = 0
                            else:
                                daily_return = (group[f'{n}日后总涨跌幅（未来函数）'].mean() + 1)*(1-m)-1  # 计算平均收益率
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
