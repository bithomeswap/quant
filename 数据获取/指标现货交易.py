import choose
import pandas as pd
import os
names = ['COIN', '股票', '指数', '行业']
# names = ['指数', '行业']

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file) & ('指标' in file) & ('排名' not in file) & ('细节' not in file) & ('分钟' not in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f'{name}.csv')
                print(name)
                df = pd.read_csv(path)
                df = df.sort_values(by='日期')  # 以日期列为索引,避免计算错误
                dates = df['日期'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
                df = df.groupby(['代码'], group_keys=False).apply(
                    choose.technology)
                # 去掉噪音数据
                for n in range(1, 9):
                    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]
                m = 0.001  # 设置手续费
                n = 6  # 设置持仓周期
                df, m, n = choose.choose('交易', name, df)
                df.to_csv(f'{name}交易细节.csv', index=False)  # 输出交易细节
                result_df = pd.DataFrame({})
                for i in range(1, n+1):
                    # 持有n天则掉仓周期为n，实际上资金实盘当中是单独留一份备用金补给亏的多的日期以及资金周转
                    days = dates[i::n]
                    daydf = df.loc[df['日期'].isin(days)]
                    # daydf.to_csv(f'{name}_{i}份交易细节.csv', index=False)  # 输出每份资金的交易细节
                    cash_balance = 1  # 初始资金设置为1元
                    result = []
                    # 每份资金的收益率
                    for date, group in daydf.groupby('日期'):
                        daily_cash_balance = {}  # 用于记录每日的资金余额
                        if group.empty:  # 如果当日没有入选标的，则收益率为0
                            daily_return = 0
                        else:
                            # daily_return = (
                                # (group[f'{n}日后总涨跌幅（未来函数）'] + 1)*(1-m)-1).mean()  # 计算平均收益率
                            # (group[f'{n}日后总涨跌幅（未来函数）'].mean()  + 1)*(1-m)-1) # 计算平均收益率，这个是不报错，但是不准确
                            for x in range(1, n+1):
                                group_return = (
                                    (group[f'{x}日后总涨跌幅（未来函数）']).mean() + 1)  # 计算平均收益率
                                if x == n:
                                    group_return = group_return*(1-m)
                                daily_cash_balance[f'未来{x}日盘中资产收益率'] = group_return

                        # # 更新资金余额并记录每日资金余额
                        # cash_balance *= (1 + daily_return)
                        # result.append(
                        #     {'日期': date, f'第{i}份资金收益率': cash_balance})
                        result.append(
                            {'日期': date, f'第{i}份资金收益率': cash_balance, f'第{i}份资金盘中资产收益率': daily_cash_balance})
                    result_df = pd.concat([result_df,  pd.DataFrame(result)])

                result_df = result_df.sort_values(by='日期')
                # 对每一份资金列分别根据对应的数据向下填充数据


                print(result_df)

                # 新建涨跌分布文件夹在上级菜单下，并保存结果
                parent_path = os.path.abspath('.')
                dir_name = '资产变动'
                path = os.path.join(parent_path, dir_name)
                if not os.path.exists(path):
                    os.makedirs(path)
                result_df.to_csv(
                    f'{path}/{name}真实收益.csv', index=False)
                print('任务已经完成！')
            except Exception as e:
                print(f"发生bug: {e}")
