import math


def technology(df):  # 定义计算技术指标的函数
    try:
        for n in range(1, 16):
            df[f'{n}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-n) / df['收盘']) - 1
            df[f'{n}日后当日涨跌（未来函数）'] = df['涨跌幅'].copy().shift(-n)+1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def choose(choosename, name, df):
    if choosename == '交易':
        # code = df['代码'].copy().drop_duplicates().tolist()  # 获取总标的数量，包含一定的未来函数
        code = df[df['日期'] == df['日期'].min()]['代码']  # 获取首日标的数量，杜绝未来函数
        rank = math.ceil(len(code)/100)
        value = math.log(len(code))
        print(name, '板块第一天的标的数量', len(code), '拟选择标的数量', rank, '阈值标准', value)
        if rank < 5:
            print(name, "标的数量过少,不适合大模型策略")
        if ('股票' not in name) & ('COIN' not in name) & ('指数' not in name) & ('行业' not in name):
            for n in (2, 9):
                df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        if ('指数' in name) | ('行业' in name):
            df = df[(df['开盘收盘幅'] >= 0.005)].copy()  # 过滤可能产生大回撤的股票
            df = df.groupby(['日期'], group_keys=True).apply(
                lambda x: x.nlargest(rank, '昨日资金波动_rank')).reset_index(drop=True)
            m = 0.005  # 设置手续费
            n = 6  # 设置持仓周期
        if ('COIN' in name):
            if ('分钟' not in name):
                w = 0.8  # 权重系数
                v = 3.2
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 过滤低价股
                df = df[df[f'昨日成交额'] >= 900000].copy()  # 过滤小盘股
                df = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= v*value/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nlargest(rank, '昨日资金波动')).reset_index(drop=True)
                m = 0.003  # 设置手续费
                n = 6  # 设置持仓周期
            if ('分钟' in name):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 过滤低价股
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 6  # 设置持仓周期
        if ('股票' in name):
            if ('分钟' not in name):
                w = 0.04
                v = 0.16
                df = df[(df['真实价格'] >= 4)].copy()  # 过滤低价股
                df = df[(df['开盘收盘幅'] <= 0.08) & (
                    df['开盘收盘幅'] >= -0.01)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= v*value/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nlargest(1, '昨日资金波动')).reset_index(drop=True)
                m = 0.005  # 设置手续费
                n = 10  # 设置持仓周期
            if ('分钟' in name):
                df = df[(df['开盘'] >= 4)].copy()  # 过滤低价股
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 15  # 设置持仓周期
        print(len(df), name)
    if choosename == '分布':
        if ('股票' not in name) & ('COIN' not in name) & ('指数' not in name) & ('行业' not in name):
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        if ('指数' in name) | ('行业' in name):
            df = df[(df['开盘收盘幅'] >= 0.005)].copy()  # 过滤可能产生大回撤的股票
            m = 0.005  # 设置手续费
            n = 6  # 设置持仓周期
        if ('COIN' in name):
            if ('分钟' not in name):
                df = df[df[f'昨日成交额'] >= 900000].copy()  # 过滤小盘股
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 过滤低价股
                m = 0.003  # 设置手续费
                n = 6  # 设置持仓周期
            if ('分钟' in name):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 过滤低价股
                m = 0.0000  # 设置手续费
                n = 6  # 设置持仓周期
        if ('股票' in name):
            n = 6  # 设置持仓周期
            if ('分钟' not in name):
                df = df[(df['开盘收盘幅'] <= 0.08) & (
                    df['开盘收盘幅'] >= -0.01)].copy()  # 过滤可能产生大回撤的股票
                df = df[(df['真实价格'] >= 4)].copy()  # 过滤低价股
                m = 0.005  # 设置手续费
                n = 15  # 设置持仓周期
            if ('分钟' in name):
                df = df[(df['开盘'] >= 4)].copy()  # 过滤低价股
                m = 0.0000  # 设置手续费
                n = 15  # 设置持仓周期
        print(name, n)
        # 对目标列进行手续费扣除
        df[f'{n}日后总涨跌幅（未来函数）'] = (df[f'{n}日后总涨跌幅（未来函数）']+1)*(1-m)-1
    return df, m, n
