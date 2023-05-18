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
        code = df['代码'].copy().drop_duplicates().tolist()  # 获取所有不重复日期
        rank = math.ceil(len(code)/100)
        print(name, '数量', len(code), '拟选择标的数量', rank)
        if ('coin' in name.lower()):
            if ('分钟' not in name.lower()):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
                df = df[(df['昨日资金波动_rank'] <= 0.5/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 2/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nsmallest(5, '昨日资金波动_rank')).reset_index(drop=True)

                m = 0.003  # 设置手续费
                n = 6  # 设置持仓周期
            if ('分钟' in name.lower()):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 6  # 设置持仓周期
        if ('股票' in name.lower()):
            if ('分钟' not in name.lower()):
                df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
                df = df[(df['开盘收盘幅'] <= 0.01)].copy()  # 开盘收盘幅过滤涨停无法买入股票
                # df = df[(df['昨日涨跌_rank'] >= 0.1) & (
                #     df[f'昨日涨跌_rank'] <= 0.9)].copy()  # 去除暴涨暴跌的标的

                df = df[(df['昨日资金波动_rank'] <= 0.5/rank)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 2/rank)].copy()
                df = df.groupby(['日期'], group_keys=True).apply(
                    lambda x: x.nsmallest(1, '昨日资金波动_rank')).reset_index(drop=True)

                m = 0.005  # 设置手续费
                n = 15  # 设置持仓周期
            if ('分钟' in name.lower()):
                df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 15  # 设置持仓周期
        else:
            for n in (2, 9):
                df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
            df = df[(df['昨日资金波动_rank'] <= 0.5/rank)].copy()
            df = df[(df['昨日资金贡献_rank'] <= 2/rank)].copy()
            df = df.groupby(['日期'], group_keys=True).apply(
                lambda x: x.nsmallest(rank*50, '昨日资金波动_rank')).reset_index(drop=True)
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        print(len(df), name)
    if choosename == '分布':
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
        if ('股票' in name.lower()):
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
        else:
            m = 0.001  # 设置手续费
            n = 6  # 设置持仓周期
        # 对目标列进行手续费扣除
        df[f'{n}日后总涨跌幅（未来函数）'] = (df[f'{n}日后总涨跌幅（未来函数）']+1)*(1-m)-1
    return df, m, n
