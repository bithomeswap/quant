import math
import pandas as pd
import os

# 设置参数
names = ['ETF', '分钟ETF']

for name in names:
    try:
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
            df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3 * (1 + n * 0.2)]
        if ('etf' in name.lower()):
            if ('分钟' not in name.lower()):
                df = df[df[f'真实价格'] >= 0.5].copy()  # 真实价格过滤劣质股票
                df = df[(df['开盘收盘幅'] <= 0.08)].copy()  # 开盘收盘幅过滤涨停无法买入股票

                df = df[(df['昨日涨跌_rank'] >= 0.5)].copy()
                for n in (2, 9):
                    if n <= 6:
                        df = df[(df[f'过去{n*5}日总涨跌_rank'] <= 0.1)].copy()
                    if n == 8:
                        df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.95)].copy()
                m = 0.001  # 设置手续费
                n = 6  # 设置持仓周期
            if ('分钟' in name.lower()):
                df = df[(df['开盘'] >= 0.5)].copy()  # 真实价格过滤劣质股票
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 6  # 设置持仓周期
            print(len(df), name)

        # 将交易标的细节输出到一个csv文件
        trading_detail_filename = f'{name}交易细节.csv'
        df.to_csv(trading_detail_filename, index=False)

        # 假设开始时有1元资金
        cash_balance = 1

        # 计算每个标的每个日期的未来当日收益
        for i in range(1, n+1):
            col = f'{i}日后总涨跌幅（未来函数）'
            df[col+'_shift'] = df.groupby('代码')[col].shift(-i)
            df[f'{i}日后当日收益'] = (df[col] + 1) / (df[col+'_shift'] + 1) - 1
        print(df)

        # 计算累加收益率和回撤
        df_grouped = df.groupby(['日期'], group_keys=False).apply(
            lambda x: x[[f'{i}日后当日收益' for i in range(1, n+1)]]).sum(axis=0).reset_index()
        df_grouped['回撤'] = df_grouped.iloc[:, 1:].cumsum(axis=1).min(axis=1)
        
        # 假设开始时有1元资金
        cash_balance = 1
        # 用于记录每日的资金余额
        daily_cash_balance = {}
        df_strategy = pd.DataFrame(columns=['日期', '执行策略'])
        df_daily_return = pd.DataFrame(columns=['日期', '收益率'])
        cash_balance_list = []
        # 记录每个交易日是否执行了策略，并输出到csv文件中
        for date, group in df.groupby('日期'):
            # 如果当日没有入选标的，则单日收益率为0
            if group.empty:
                daily_return = 0
            else:
                daily_return = ((group[f'{n}日后总涨跌幅（未来函数）'] + 1).mean()*(1-m)-1)/n  # 计算平均收益率
            df_daily_return = pd.concat(
                [df_daily_return, pd.DataFrame({'日期': [date], '收益率': [daily_return]})])
            # 更新资金余额并记录每日资金余额
            cash_balance *= (1 + daily_return)
            daily_cash_balance[date] = cash_balance
            cash_balance_list.append(cash_balance)  # 添加每日资金余额到列表中
        df_cash_balance = pd.DataFrame({'日期': list(daily_cash_balance.keys()), '资金余额': list(daily_cash_balance.values())})
        result_df = pd.merge(df_daily_return, df_cash_balance, on='日期')
        # 新建涨跌分布文件夹在上级菜单下，并保存结果
        parent_path = os.path.abspath('.')
        dir_name = '资产变动'
        dir_path = os.path.join(parent_path, dir_name)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        result_df.to_csv(f'{dir_path}/{name}资产变动.csv')
        print('任务已经完成！')
    except Exception as e:
        print(f"发生bug: {e}")

