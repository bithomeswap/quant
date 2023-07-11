import pandas as pd
import numpy as np
import choose
import os
import datetime
from pymongo import MongoClient

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# names = ["可转债","COIN", "股票", "指数", "行业", "ETF",]
names = ["股票", ]
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f"{name}.csv")
                print(name)
                df = pd.read_csv(path)
                df['涨跌幅'] = df['开盘'].shift(-1)/df['昨收'].shift(-1)-1
                # filtered_columns = [col for col in df.columns if "rank" not in col]
                # df = df[filtered_columns]
                watchtime = 1999
                # if ("股票" in name) and ("可转债" not in name):  # 数据截取
                #     watchtime = 2017
                #     start_date = datetime.datetime(
                #         watchtime, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                #     end_date = datetime.datetime(datetime.datetime.strptime(
                #         start_date, "%Y-%m-%d %H:%M:%S").year + 8, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                #     df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
                # else:  # 数据截取
                #     watchtime = 2018
                #     start_date = datetime.datetime(
                #         watchtime, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                #     end_date = datetime.datetime(datetime.datetime.strptime(
                #         start_date, "%Y-%m-%d %H:%M:%S").year + 8, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                #     df = df[df["日期"] >= start_date]
                #     df = df[df["日期"] <= end_date]
                # # 不免手续费的话，将近五十多倍的收益（收益高的离谱很可能是有借壳上市的停牌期间的利润）
                # df = df[df['总市值_rank'] > 0.99].copy()
                # # 加上基本面过滤垃圾标的之后收益率有提高，各排除百分之五就已经达到65倍收益了
                # df = df[df['市销率_rank'] < 0.95].copy()
                # df = df[df['市盈率_rank'] < 0.95].copy()
                # df = df[df['市盈率_rank'] < 0.95].copy()
                # df = df[df['收盘_rank'] > 0.05].copy()
                df = df.groupby("代码", group_keys=False).apply(choose.technology)
                df = df[df[f"收盘"] > 4].copy()
                df = df.groupby("代码", group_keys=False).apply(choose.rank)
                df.to_csv(f'股票指标（收益率隔夜）{name}.csv')
                df, m, n = choose.choose("分布", name, df)
                if ("股票" in name):
                    for i in range(1, n+1):
                        df = df[df[f"{i}日后总涨跌幅（未来函数）"] <= 3*(1+0.1*n)]
                else:
                    for i in range(1, n+1):
                        df = df[df[f"{i}日后总涨跌幅（未来函数）"] <= 20*(1+0.1*n)]
                # 将数据划分成a个等长度的区间
                a = 50
                ranges = []
                left = 0
                right = 1
                step = (right - left) / a
                for i in range(a):
                    ranges.append((left + i * step, left + (i + 1) * step))
                # 筛选出列名中包含"rank"的列
                rank_cols = df.filter(like="rank").columns.tolist()
                # 创建空的结果DataFrame
                result_df = pd.DataFrame()
                # 循环处理每个指标和区间
                for rank_range in ranges:
                    col_result_df = pd.DataFrame()  # 创建一个空的DataFrame，用于存储指标的结果
                    for col_name in rank_cols:
                        # 根据区间筛选DataFrame
                        sub_df = df[(df[col_name] >= rank_range[0]) &
                                    (df[col_name] <= rank_range[1])]
                        # 计算收益
                        sub_df_mean = sub_df.mean(numeric_only=True)  # 均值法
                        # 构造包含指标名和涨跌幅的DataFrame，并添加到列结果DataFrame中
                        result_sub_df = pd.DataFrame(
                            {col_name: [sub_df_mean[f"{n}日后总涨跌幅（未来函数）"]]}, index=[rank_range])
                        col_result_df = pd.concat(
                            [col_result_df, result_sub_df], axis=1)
                    result_df = pd.concat([result_df, col_result_df])
                # 新建涨跌分布文件夹在上级菜单下，并保存结果
                path = os.path.join(os.path.abspath("."), "资产多指标排名收益分布")
                if not os.path.exists(path):
                    os.makedirs(path)
                result_df.to_csv(
                    f"{path}/{name}持有{n}日{str(watchtime)}年多指标排名收益分布.csv")
                print("任务已经完成！")
            except Exception as e:
                print(f"发生bug: {e}")
