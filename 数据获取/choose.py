import math
import pandas as pd


def technology(df):  # 定义计算技术指标的函数
    slippage = 0.001  # 设置滑点千分之一
    commission = 0.0013  # 设置滑点千分之一
    try:
        df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
        if "换手率" in df.columns:
            df['涨跌幅（原值）'] = df['涨跌幅']+1
            for n in range(1, 50):
                if n == 1:
                    df[f"{n}日后总涨跌幅（未来函数）"] = df['涨跌幅（原值）'].shift(-1)-1
                if n > 1:
                    df[f"{n}日后总涨跌幅（未来函数）"] = (
                        (df[f"{n-1}日后总涨跌幅（未来函数）"]+1)*df['涨跌幅（原值）'].shift(-n))-1
            # for n in range(1, 81):
            #     # 计算加滑点之后的收益，A股扣一分钱版本
            #     df["买入（未来函数）"] = df["开盘"].apply(
            #         lambda x: math.ceil(x * (1 + slippage) * 100) / 100)
            #     df["卖出（未来函数）"] = df["开盘"].apply(lambda x: math.floor(
            #         x * (1 - slippage) * (1 - commission) * 100) / 100)
            #     df[f"{n}日后总涨跌幅（未来函数）"] = (
            #         df["卖出（未来函数）"].copy().shift(-n) / df["买入（未来函数）"]) - 1
        # else:
        #     for n in range(1, 81):
        #         # 计算不加滑点的收益
        #         df[f"{n}日后总涨跌幅（未来函数）"] = (
        #             df["收盘"].copy().shift(-n) / df["收盘"]) - 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def rank(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if ("未来函数" not in str(column)):
            df = pd.concat([df, (df[str(column)].rank(
                method="max", ascending=False) / len(df)).rename(f"{str(column)}_rank")], axis=1)
    return df


def choose(choosename, name, df):
    df = df.sort_values(by="日期")  # 以日期列为索引,避免计算错误
    if choosename == "交易":
        code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
        num = math.ceil(len(code)/100)
        print(name, "板块第一天的标的数量", len(code), "择股数量", num)
        if ("可转债" not in name):  # 数据截取
            if ("分钟" not in name):
                df = df[(df["收盘"] >= 4)].copy()  # 过滤垃圾股
                # # 不免手续费的话，将近五十多倍的收益（收益高的离谱很可能是有借壳上市的停牌期间的利润）
                df = df.groupby("日期", group_keys=True).apply(lambda x: x.nlargest(5, f"总市值_rank")).reset_index(drop=True)
                df = df[(df["涨跌幅（开盘原值）"] <= 0.09)].copy()  # 过滤垃圾股
                # df = df[df['总市值_rank'] > 0.995]
                # df = df[df['资金波动_rank'] <0.005] # 不如单独的市值效果好
                # df = df[df['收盘_rank'] > 0.2]
                # df = df[df['成交额_rank'] > 0.3]
                # df = df[df['振幅_rank']>0.2]
                # df = df[df['换手率_rank']>0.2]
                # df = df[(df['涨跌幅_rank'] < 0.92) & (df['涨跌幅_rank'] > 0.02)]

                # # 免手续费的话，大概二十六倍的收益（收益有点低）
                # # df["score"] = 0
                # # df["score"] += df[f"总市值_rank"].apply(lambda x: (x >= 0.98)).astype(int)
                # # df["score"] += df[f"收盘_rank"].apply(lambda x: (x >= 0.98)).astype(int)
                # # df["score"] += df[f"市盈率_rank"].apply(lambda x: (x <= 0.01)).astype(int)
                # # df["score"] += df[f"市净率_rank"].apply(lambda x: (x <= 0.01)).astype(int)
                # # df["score"] += df[f"市销率_rank"].apply(lambda x: (x <= 0.01)).astype(int)
                # # df = df.groupby("日期", group_keys=True).apply(lambda x: x.nlargest(10, f"score")).reset_index(drop=True)

                m = 0.005  # 设置手续费
                n = 30  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 4)].copy()  # 过滤垃圾股
                for n in (2, 9):
                    df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 30  # 设置持仓周期
        if ("可转债" in name):
            df = df[(df["涨跌幅"] <= 0.09)].copy()  # 过滤垃圾股
            for n in range(1, 5):
                df = df[df[f"过去{n}日成交额_rank"] > 0.5].copy()
                df = df[df[f"过去{n}日资金波动_rank"] < 0.5].copy()
            df = df[(df[f"过去1日资金波动_rank"] <= 0.1)].copy()  # 过滤垃圾股
            df = df.groupby("日期", group_keys=True).apply(
                lambda x: x.nsmallest(1, f"开盘")).reset_index(drop=True)
            m = 0.0005  # 设置手续费
            n = 80  # 设置持仓周期
        print(len(df), name)
    if choosename == "分布":
        if ("可转债" not in name):  # 数据截取
            if ("分钟" not in name):
                df = df[(df["开盘"] >= 4)].copy()  # 过滤垃圾股
                df = df[(df["涨跌幅（开盘原值）"] <= 0.09)].copy()  # 过滤垃圾股
                m = 0.01  # 设置手续费
                n = 30  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 4)].copy()  # 过滤垃圾股
                m = 0.0000  # 设置手续费
                n = 30  # 设置持仓周期
        if ("可转债" in name):
            df = df[(df["涨跌幅"] <= 0.09)].copy()  # 过滤垃圾股
            m = 0.0005  # 设置手续费
            n = 190  # 设置持仓周期
        print(name, n)
        # 对目标列进行手续费扣除
        df[f"{n}日后总涨跌幅（未来函数）"] = (df[f"{n}日后总涨跌幅（未来函数）"]+1)*(1-m)-1
    return df, m, n
