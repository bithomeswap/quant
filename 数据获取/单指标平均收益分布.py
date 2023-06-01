import choose
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime
import os
# 设置参数
names = ["COIN", "股票", "指数", "行业"]
mubiao = f"涨跌幅"
a = 20  # 将数据划分成a个等距离的区间
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file) & ("指标" in file) & ("排名" not in file) & ("细节" not in file):
            try:
                print(f"{mubiao}")
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f"{name}.csv")
                print(name)
                df = pd.read_csv(path)
                watchtime = all
                if ("COIN" in name):
                    watchtime = 2021
                    start_date = datetime.datetime(
                        watchtime, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    end_date = datetime.datetime(datetime.datetime.strptime(
                        start_date, "%Y-%m-%d %H:%M:%S").year + 1, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    df = df[df["日期"] >= start_date]
                    df = df[df["日期"] <= end_date]
                if ("股票" in name)&("分组" not in name):
                    watchtime = 2019
                    start_date = datetime.datetime(
                        watchtime, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    end_date = datetime.datetime(datetime.datetime.strptime(
                        start_date, "%Y-%m-%d %H:%M:%S").year + 1, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
                df = df.groupby(["代码"], group_keys=False).apply(choose.technology)
                # 去掉n日后总涨跌幅大于百分之三百的噪音数据
                for n in range(1, 9):
                    df = df[df[f"{n}日后总涨跌幅（未来函数）"] <= 3*(1+n*0.2)]
                df["日期"] = pd.to_datetime(
                    df["日期"], format="%Y-%m-%d")  # 转换日期格式
                df, m, n = choose.choose("分布", name, df)
                df = df.dropna()
                # df.to_csv(f"实际统计数据{name}_{mubiao}_{watchtime}年.csv")
                sorted_data = np.sort(df[f"{mubiao}"])
                indices = np.linspace(
                    0, len(df[f"{mubiao}"]), num=a+1, endpoint=True, dtype=int)
                # 得到每一个区间的上界，并作为该部分对应的区间范围
                ranges = []
                for i in range(len(indices) - 1):
                    start_idx = indices[i]
                    end_idx = indices[i+1] if i != len(indices) - \
                        2 else len(df[f"{mubiao}"])  # 最后一段需要特殊处理
                    upper_bound = sorted_data[end_idx-1]  # 注意索引从0开始，因此要减1
                    ranges.append((sorted_data[start_idx], upper_bound))
                result_dicts = []
                for n in range(1, 10):
                    for rank_range in ranges:
                        sub_df = df.copy()[(df[f"{mubiao}"] >= rank_range[0]) &
                                           (df[f"{mubiao}"] <= rank_range[1])]

                        count = len(sub_df)
                        future_returns = np.array(sub_df[f"{n}日后总涨跌幅（未来函数）"])
                        # 括号注意大小写的问题，要不就会报错没这个参数
                        up_rate = len(
                            future_returns[future_returns >= 0]) / len(future_returns)
                        avg_return = np.mean(future_returns)
                        result_dict = {
                            f"{mubiao}": f"from{rank_range[0]}to{rank_range[1]}",
                            f"{n}日统计次数（已排除涨停）": count,
                            f"未来{n}日上涨概率": up_rate,
                            f"未来{n}日上涨次数": len(future_returns[future_returns >= 0]),
                            f"未来{n}日平均涨跌幅": avg_return,
                        }
                        result_dicts.append(result_dict)
                # 将结果持久化
                result_df = pd.DataFrame(result_dicts)
                for n in range(1, 10):
                    cols_to_shift = [f"{n}日统计次数（已排除涨停）",
                                     f"未来{n}日上涨概率", f"未来{n}日上涨次数", f"未来{n}日平均涨跌幅"]
                    result_df[cols_to_shift] = result_df[cols_to_shift].shift(
                        -a*(n-1))
                # result_df = result_df.dropna()  # 删除含有空值的行
                path = os.path.join(os.path.abspath("."), "资产单指标平均收益分布")
                if not os.path.exists(path):
                    os.makedirs(path)
                result_df.round(decimals=6).to_csv(
                    f"{path}/{name}{mubiao}{str(watchtime)}年平均收益分布.csv", index=False)
                print(name, "已完成")
            except Exception as e:
                print(f"发生bug: {e}")
