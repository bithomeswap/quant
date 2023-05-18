import pandas as pd
import numpy as np
import choose
import os
names = ['COIN','股票','指数','行业','ETF']
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
                df = df.groupby(['代码'], group_keys=False).apply(choose.technology)
                # 去掉噪音数据
                for n in range(1, 9):
                    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]
                df, m, n = choose.choose('分布',name, df)
                print(name,n)
                # 将数据划分成a个等长度的区间
                a = 50
                ranges = []
                left = 0
                right = 1
                step = (right - left) / a
                for i in range(a):
                    ranges.append((left + i * step, left + (i + 1) * step))
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
                            {col_name: [sub_df_mean[f'{n}日后总涨跌幅（未来函数）']]}, index=[rank_range])
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
