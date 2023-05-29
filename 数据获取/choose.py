import math


def technology(df):  # 定义计算技术指标的函数
    try:
        for n in range(1, 46):
            df[f"{n}日后总涨跌幅（未来函数）"] = (df["收盘"].copy().shift(-n) / df["收盘"]) - 1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def choose(choosename, name, df):
    if choosename == "交易":
        code = df[df["日期"] == df["日期"].min()]["代码"]  # 获取首日标的数量，杜绝未来函数
        value = math.floor(math.log10(len(code)))  # 整数位数
        rank = math.ceil(len(code)/(10**value))  # 持仓数量
        print(name, "板块第一天的标的数量", len(code), "整数位数", value, "择股数量", rank)
        if ("股票" not in name) & ("COIN" not in name) & ("指数" not in name) & ("行业" not in name):
            for n in (2, 9):
                df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        if ("指数" in name) | ("行业" in name):
            # df = df[df[f'过去{5}日涨跌{0}金叉'] == 1].copy()
            # df=df[df[f"指数{5}日偏离"]>df[f"指数{2}日偏离"]]
            df = df.groupby(["日期"], group_keys=True).apply(lambda x: x.nlargest(rank, "昨日资金波动")).reset_index(drop=True)
            m = 0.005  # 设置手续费
            n = 30  # 设置持仓周期
        if ("COIN" in name):
            if ("分钟" not in name):
                df = df[df[f"开盘"] >= 0.00001000].copy()  # 过滤低价股
                df = df[(df["昨日资金波动_rank"] <= 0.01)].copy()
                df = df.groupby(["日期"], group_keys=True).apply(lambda x: x.nsmallest(rank, "开盘")).reset_index(drop=True)
                m = 0.005  # 设置手续费
                n = 45  # 设置持仓周期
            if ("分钟" in name):
                df = df[df[f"开盘"] >= 0.00001000].copy()  # 过滤低价股
                for n in (2, 9):
                    df = df[(df[f"过去{n}日总涨跌_rank"] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 45  # 设置持仓周期
        if ("股票" in name):  # 股票数量在三千以下的时候适合这个数值计算方法，如果数量过多的话，把数据拆分成这种数据会比较厚
            if ("分钟" not in name):
                df = df[(df["真实价格"] >= 4)].copy()  # 过滤低价股
                df = df[(df["开盘收盘幅"] <= 0.08)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df["昨日资金波动_rank"] <= 0.01)].copy()
                df = df.groupby(["日期"], group_keys=True).apply(
                    lambda x: x.nsmallest(rank, "昨日总市值")).reset_index(drop=True)
                m = 0.005  # 设置手续费
                n = 30  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 4)].copy()  # 过滤低价股
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
            df = df[(df["开盘收盘幅"] >= 0.005)].copy()  # 过滤可能产生大回撤的股票
            m = 0.005  # 设置手续费
            n = 6  # 设置持仓周期
        if ("COIN" in name):
            if ("分钟" not in name):
                df = df[df[f"开盘"] >= 0.00001000].copy()  # 过滤低价股
                m = 0.005  # 设置手续费
                n = 45  # 设置持仓周期
            if ("分钟" in name):
                df = df[df[f"开盘"] >= 0.00001000].copy()  # 过滤低价股
                m = 0.0000  # 设置手续费
                n = 45  # 设置持仓周期
        if ("股票" in name):
            # 之前是15天比较好，其实理论上越长越好，但是没实际跑数据，这次跑了次24，比15有提高
            if ("分钟" not in name):
                df = df[(df["开盘收盘幅"] <= 0.08)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df["真实价格"] >= 4)].copy()  # 过滤低价股
                m = 0.005  # 设置手续费
                n = 30  # 设置持仓周期
            if ("分钟" in name):
                df = df[(df["开盘"] >= 4)].copy()  # 过滤低价股
                m = 0.0000  # 设置手续费
                n = 30  # 设置持仓周期
        print(name, n)
        # 对目标列进行手续费扣除
        df[f"{n}日后总涨跌幅（未来函数）"] = (df[f"{n}日后总涨跌幅（未来函数）"]+1)*(1-m)-1
    return df, m, n
