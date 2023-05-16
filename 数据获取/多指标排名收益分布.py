import pandas as pd
import numpy as np
import datetime
import os
# 设置参数# 000（深证核心）、002（深证中小）、600（上证核心）、603（上证创新）、001（深证创新）、601（上证改革）、605（上证中小）
names = ['000', '002', '600',  '603', ('001', '601', '605'), '指数', 'ETF', 'COIN']
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
                # 去掉噪音数据
                for n in range(1, 9):
                    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]
                m = 0.001  # 设置手续费
                n = 6  # 设置持仓周期
                if ('etf' in name.lower()):
                    if ('分钟' not in name.lower()):
                        df = df[df[f'真实价格'] >= 0.8].copy()  # 开盘价过滤高滑点股票
                        m = 0.003  # 设置手续费
                        n = 6  # 设置持仓周期
                if ('coin' in name.lower()):
                    if ('分钟' not in name.lower()):
                        df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                        df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
                        m = 0.003  # 设置手续费
                        n = 6  # 设置持仓周期
                    if ('分钟' in name.lower()):
                        df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                        m = 0.0000  # 设置手续费
                        n = 6  # 设置持仓周期

                if ('00' in name.lower()) or ('60' in name.lower()):
                    n = 6  # 设置持仓周期
                    if ('分钟' not in name.lower()):
                        df = df[(df['开盘收盘幅'] <= 0.01)].copy()  # 开盘收盘幅过滤涨停无法买入股票
                        df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
                        m = 0.005  # 设置手续费
                        n = 15  # 设置持仓周期
                    if ('分钟' in name.lower()):
                        df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
                        m = 0.0000  # 设置手续费
                        n = 15  # 设置持仓周期
                mubiao = f'{n}日后总涨跌幅（未来函数）'
                # 将数据划分成a个等长度的区间
                a = 20
                ranges = []
                left = 0
                right = 1
                step = (right - left) / a
                for i in range(a):
                    ranges.append((left + i * step, left + (i + 1) * step))
                # 对目标列进行手续费扣除
                df[mubiao] = (df[mubiao]+1)*(1-m)-1
                # 筛选出列名中包含'rank'的列
                rank_cols = df.filter(like='rank').columns.tolist()
                # 创建空的结果DataFrame
                result_df = pd.DataFrame()
                # 循环处理每个指标和区间
                for rank_range in ranges:
                    col_result_df = pd.DataFrame()  # 创建一个空的DataFrame，用于存储指标的结果
                    for col_name in rank_cols:
                        # 根据区间筛选DataFrame
                        sub_df = df[(df[col_name] >= rank_range[0]) &
                                    (df[col_name] <= rank_range[1])]
                        # 计算均值
                        sub_df_mean = sub_df.mean(numeric_only=True)
                        # 构造包含指标名和涨跌幅的DataFrame，并添加到列结果DataFrame中
                        result_sub_df = pd.DataFrame(
                            {col_name: [sub_df_mean[mubiao]]}, index=[rank_range])
                        col_result_df = pd.concat(
                            [col_result_df, result_sub_df], axis=1)
                    result_df = pd.concat([result_df, col_result_df])
                # 新建涨跌分布文件夹在上级菜单下，并保存结果
                parent_path = os.path.abspath('.')
                dir_name = '涨跌分布'
                path = os.path.join(parent_path, dir_name)
                if not os.path.exists(path):
                    os.makedirs(path)
                result_df.to_csv(f'{path}/{name}_{n}日指标排名平均收益.csv')
                print('任务已经完成！')
            except Exception as e:
                print(f"发生bug: {e}")
