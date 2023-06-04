import math
import choose
import pandas as pd
import os
import datetime
from pymongo import MongoClient

client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# names = ["COIN", "股票", "指数", "行业"]
names = ["股票"]

moneyused = 0.9  # 设置资金利用率

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file) & ("指标" in file) & ("排名" not in file) & ("细节" not in file) & ("分钟" not in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f"{name}.csv")
                df = pd.read_csv(path)
                if ("COIN" in name):
                    n = 2021
                    start_date = datetime.datetime(
                        n, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    end_date = datetime.datetime(datetime.datetime.strptime(
                        start_date, "%Y-%m-%d %H:%M:%S").year + 3, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    df = df[df["日期"] >= start_date]
                    df = df[df["日期"] <= end_date]
                if "股票" in name:  # 数据截取
                    n = 2017
                    start_date = datetime.datetime(n, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    end_date = datetime.datetime(datetime.datetime.strptime(
                        start_date, "%Y-%m-%d %H:%M:%S").year + 8, int(1), int(1)).strftime("%Y-%m-%d %H:%M:%S")
                    df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]

                df = df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
                dates = df["日期"].copy().drop_duplicates().tolist()  # 获取所有不重复日期
                df = df.groupby(["代码"], group_keys=False).apply(choose.technology)
                if ("股票" in name):
                    df = pd.merge(df, pd.DataFrame(list(db[f"非ST股票('000', '001', '002', '600', '601', '603', '605')"].find())), on=['代码', '日期'], how='inner')
                    df.dropna
                print(df)
                # 分组并计算指标排名
                df = df.groupby(["日期"], group_keys=False).apply(choose.rank)
                df.to_csv(f"最终版指标{name}")
                df, m, n = choose.choose("交易", name, df)
                if ("COIN" in name):
                    for i in range(1, n+1):
                        df = df[df[f"{i}日后总涨跌幅（未来函数）"] <= 20*(1+0.1*n)]
                if ("股票" in name):
                    for i in range(1, n+1):
                        df = df[df[f"{i}日后总涨跌幅（未来函数）"] <= 3*(1+0.1*n)]

                trade_path = os.path.join(os.path.abspath("."), "资产交易细节")
                if not os.path.exists(trade_path):
                    os.makedirs(trade_path)
                df.to_csv(f"{trade_path}/{name}周期{n}交易细节.csv", index=False)
                print("交易细节已输出")

                result_df = pd.DataFrame({})
                for i in range(1, n+1):
                    # 持有n天则掉仓周期为n，实际上资金实盘当中是单独留一份备用金补给亏的多的日期以及资金周转
                    days = dates[i::n+1]
                    daydf = df[df["日期"].isin(days)]

                    # tradedaypath = os.path.join(os.path.abspath("."), "资产分组交易细节")
                    # if not os.path.exists(tradedaypath):
                    #     os.makedirs(tradedaypath)
                    # daydf.to_csv(f"{tradedaypath}/{name}周期{i}交易细节.csv", index=False)

                    result = []
                    cash_balance = 1  # 初始资金设置为1元
                    twocash_balance = 1
                    # 每份资金的收益率
                    for date, group in daydf.groupby("日期"):
                        daily_cash_balance = {}  # 用于记录每日的资金余额
                        if not group.empty:  # 如果当日没有入选标的，则收益率为0
                            for x in range(1, n+1):
                                if x < n:
                                    group_return = (
                                        (group[f"{x}日后总涨跌幅（未来函数）"]).mean() + 1)  # 计算平均收益率
                                if x == n:
                                    group_return = group_return*(1-m)
                                    # 复投累计收益率
                                    cash_balance *= (group_return-1) * \
                                        moneyused+1
                                    # 不复投累计收益率
                                    twocash_balance += (group_return-1) * \
                                        moneyused
                                daily_cash_balance[f"未来{x}日盘中资产收益"] = group_return
                        daily_cash_balance[f"下周期余额复投"] = cash_balance
                        daily_cash_balance[f"下周期余额不复投"] = twocash_balance
                        result.append(
                            {"日期": date, f"第{i}份资金盘中资产收益": daily_cash_balance})
                    result_df = pd.concat([result_df, pd.DataFrame(result)])

                manyday = pd.DataFrame({"日期": dates})
                manyday = manyday[~manyday["日期"].isin(df["日期"])]
                # 将两个数据集根据key列进行合并
                result_df = pd.concat([result_df,  manyday])
                result_df = result_df.sort_values(
                    by="日期").reset_index(drop=True)

                for i in range(1, n+1):  # 对每一份资金列分别根据对应的数据向下填充数据
                    cash = 1
                    twocash = 1
                    firstcash = 1000000  # 假设初始资金每天分配100万元
                    daysindex = result_df[result_df[f"第{i}份资金盘中资产收益"].notna()].copy()[
                        "日期"]
                    for dayindex in daysindex:
                        fill = result_df[result_df["日期"]
                                         == dayindex][f"第{i}份资金盘中资产收益"]
                        for col, val in fill.items():
                            for x, (colnum, value) in enumerate(val.items()):
                                if x < n:
                                    # print(col, val, colnum, value)
                                    result_df.at[col+x+1,
                                                 f"第{i}份资金周期资产收益"] = value
                                    result_df.at[col+x+1,
                                                 f"第{i}份资金累积资产收益复投"] = value*cash
                                    result_df.at[col+x+1,
                                                 f"第{i}份资金累积资产收益不复投"] = value*twocash
                                if x == n:
                                    nextcash = value
                                    cash = nextcash
                                if x == n+1:
                                    twonextcash = value
                                    twocash = twonextcash
                for i in range(1, n+1):  # 对每一份资金列分别根据对应的数据向下填充数据
                    result_df[f"第{i}份资金周期资产收益"] = result_df[f"第{i}份资金周期资产收益"].fillna(
                        1)
                    result_df[f"第{i}份资金累积资产收益复投"] = result_df[f"第{i}份资金累积资产收益复投"].fillna(
                        method="ffill").fillna(1)
                    result_df[f"第{i}份资金累积资产收益不复投"] = result_df[f"第{i}份资金累积资产收益不复投"].fillna(
                        method="ffill").fillna(1)
                # 使用 filter() 方法选择所有包含指定子字符串的列
                retrade = result_df.filter(like="资金累积资产收益复投").columns
                result_df["总资产收益率（复投）%"] = (
                    result_df[retrade].mean(axis=1)-1)*100
                notretrade = result_df.filter(like="资金累积资产收益不复投").columns
                result_df["总资产收益率（不复投）%"] = (
                    result_df[notretrade].mean(axis=1)-1)*100
                print("持仓周期", n, "复投收益率", result_df["总资产收益率（复投）%"])
                result_df["日期"] = result_df["日期"].str.replace('-', '||')
                # 新建涨跌分布文件夹在上级菜单下，并保存结果
                path = os.path.join(os.path.abspath("."), "资产变动")
                if not os.path.exists(path):
                    os.makedirs(path)
                result_df.to_csv(f"{path}/{name}周期{n}真实收益.csv", index=False)
                print("任务已经完成！")
            except Exception as e:
                print(f"发生bug: {e}")
