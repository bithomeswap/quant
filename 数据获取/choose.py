def choose(name, df):
    m = 0.001  # 设置手续费
    n = 6  # 设置持仓周期
    if ('etf' in name.lower()):  # 绝大部分基金靠收管理费赚钱，并不靠净值分红赚钱，分红赚的不如接盘私募多，所以这种衍生品比较弱势
        if ('分钟' not in name.lower()):
            df = df[df[f'真实价格'] >= 0.5].copy()  # 真实价格过滤劣质股票
            # 开盘收盘幅过滤涨停无法买入股票
            df = df[(df['开盘收盘幅'] <= 0.08)].copy()
            df = df[(df['昨日涨跌_rank'] >= 0.5)].copy()
            df = df[(df[f'过去{40}日总涨跌_rank'] >= 0.9)].copy()
            for n in range(2, 4):
                df = df[(df[f'过去{n}日总涨跌'] <= 1)].copy()
            m = 0.005  # 设置手续费
            n = 4  # 设置持仓周期
    if ('coin' in name.lower()):
        if ('分钟' not in name.lower()):
            df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
            df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
            df = df[(df['开盘_rank'] >= 0.5)].copy()  # 真实价格过滤劣质股票
            df = df[(df['昨日资金波动_rank'] <= 0.15)].copy()
            df = df[(df['昨日资金贡献_rank'] <= 0.15)].copy()
            # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
            # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
            m = 0.003  # 设置手续费
            n = 6  # 设置持仓周期
        if ('分钟' in name.lower()):
            df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
            for n in (2, 9):
                df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
            m = 0.0000  # 设置手续费
            n = 6  # 设置持仓周期
    if ('00' in name.lower()) or ('60' in name.lower()):
        if ('分钟' not in name.lower()):
            df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
            # 开盘收盘幅过滤涨停无法买入股票
            df = df[(df['开盘收盘幅'] <= 0.01)].copy()
            df = df[(df['真实价格_rank'] <= 0.8) & (
                df['真实价格_rank'] >= 0.2)].copy()  # 真实价格过滤劣质股票
            df = df[(df['昨日资金波动_rank'] <= 0.05)].copy()
            df = df[(df['昨日资金贡献_rank'] <= 0.05)].copy()
            # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
            # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
            m = 0.005  # 设置手续费
            n = 15  # 设置持仓周期
        if ('分钟' in name.lower()):
            df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
            for n in (2, 9):
                df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
            m = 0.0000  # 设置手续费
            n = 15  # 设置持仓周期
    print(len(df), name)
    return df
