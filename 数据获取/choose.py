import math


def technology(df):  # 定义计算技术指标的函数
    slippage = 0.001  # 设置滑点千分之一
    commission = 0.0013  # 设置滑点千分之一
    try:
        for n in range(1, 46):
            # if "换手率" in df.columns:
            #     # 计算加滑点之后的收益，A股扣一分钱版本
            #     df["买入"] = df["开盘"].apply(
            #         lambda x: math.ceil(x * (1 + slippage) * 100) / 100)
            #     df["卖出"] = df["开盘"].apply(lambda x: math.floor(
            #         x * (1 - slippage) * (1 - commission) * 100) / 100)
            #     df[f"{n}日后总涨跌幅（未来函数）"] = (
            #         df["卖出"].copy().shift(-n) / df["买入"]) - 1
            # else:
            # 计算不加滑点的收益
            df[f"{n}日后总涨跌幅（未来函数）"] = (
                df["收盘"].copy().shift(-n) / df["收盘"]) - 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def choose(choosename, name, df):
    if choosename == "交易":
        code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
        num = math.ceil(len(code)/100)
        print(name, "板块第一天的标的数量", len(code), "择股数量", num)
        if ("股票" not in name) & ("COIN" not in name) & ("指数" not in name) & ("行业" not in name):
            for n in (2, 9):
                df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        if ("指数" in name) | ("行业" in name):
            df = df.groupby(["日期"], group_keys=True).apply(
                lambda x: x.nlargest(1, f"过去{1}日资金波动")).reset_index(drop=True)
            m = 0.005  # 设置手续费
            n = 30  # 设置持仓周期
        if ("COIN" in name):
            if ("分钟" not in name):
                df = df[(df[f"开盘"] >= 0.00000200)].copy()  # 过滤垃圾股
                df = df[(df[f"过去{1}日资金波动_rank"] <= 0.01)].copy()
                df = df.groupby(["日期"], group_keys=True).apply(
                    lambda x: x.nsmallest(1, f"开盘")).reset_index(drop=True)
                m = 0.01  # 设置手续费
                n = 45  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df[f"开盘"] >= 0.00000200)].copy()  # 过滤垃圾股
                for n in (2, 9):
                    df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 45  # 设置持仓周期
        if ("股票" in name):
            if ("分钟" not in name):
                df = df[(df["开盘"] >= 2) & (df["涨跌幅"] <= 0.09)].copy()  # 过滤垃圾股
                df = df[(df[f"过去{1}日资金波动_rank"] <= 0.01)].copy()
                # df = df[(df[f"过去{1}日资金波动_rank"] <= 0.01)|(df[f"过去{2}日资金波动_rank"] <= 0.01)|(df[f"过去{3}日资金波动_rank"] <= 0.01)].copy()
                df = df.groupby(["日期"], group_keys=True).apply(
                    lambda x: x.nsmallest(1, f"开盘")).reset_index(drop=True)
                m = 0.01  # 设置手续费
                n = 30  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 2)].copy()  # 过滤垃圾股
                for n in (2, 9):
                    df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 30  # 设置持仓周期
        print(len(df), name)
    if choosename == "分布":
        if ("股票" not in name) & ("COIN" not in name) & ("指数" not in name) & ("行业" not in name):
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        if ("指数" in name) | ("行业" in name):
            m = 0.005  # 设置手续费
            n = 6  # 设置持仓周期
        if ("COIN" in name):
            if ("分钟" not in name):
                df = df[(df[f"开盘"] >= 0.00000200)].copy()  # 过滤垃圾股
                m = 0.01  # 设置手续费
                n = 45  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df[f"开盘"] >= 0.00000200)].copy()  # 过滤垃圾股
                m = 0.0000  # 设置手续费
                n = 45  # 设置持仓周期
        if ("股票" in name):
            if ("分钟" not in name):
                df = df[(df["开盘"] >= 2) & (df["涨跌幅"] <= 0.09)].copy()  # 过滤垃圾股
                m = 0.005  # 设置手续费
                n = 45  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 2)].copy()  # 过滤垃圾股
                m = 0.0000  # 设置手续费
                n = 45  # 设置持仓周期
        print(name, n)
        # 对目标列进行手续费扣除
        df[f"{n}日后总涨跌幅（未来函数）"] = (df[f"{n}日后总涨跌幅（未来函数）"]+1)*(1-m)-1
    return df, m, n
