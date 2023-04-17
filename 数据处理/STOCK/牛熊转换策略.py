import pandas as pd

# 读取数据
name = 'STOCK_20140101_20170101'
df = pd.read_csv(f'{name}指标.csv')

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

# 按日期遍历所有交易日
all_trading_detail = pd.DataFrame()  # 存储所有交易细节
all_daily_return = pd.DataFrame()  # 存储每日收益率和净资产走势
cash_balance = 10000  # 初始资金
daily_cash_balance = {}  # 存储每日资金余额
fees = 0.003  # 手续费
holding_period = 8  # 持仓周期

for date in sorted(df['日期'].unique()):
    group = df[df['日期'] == date]  # 当天的股票
    ema121_ratio_product = group['EMA121开盘比值'].prod()  # 当天所有股票EMA121开盘比值的乘积
    if ema121_ratio_product > 1.1:  # 牛市策略
        for stock in group['股票代码']:  # 买入所有当天的股票
            cash_balance -= fees * cash_balance  # 扣除手续费
            buy_price = group[group['股票代码'] ==
                              stock]['收盘价'].values[0]  # 买入价格为当天收盘价
            holding_days = 1  # 已持有天数
            for i in range(1, holding_period+1):
                # 如果未来函数中没有数据了，则退出持仓
                if date + pd.Timedelta(days=i) not in df['日期'].unique():
                    break
                fut_price = df[(df['日期'] == date + pd.Timedelta(days=i))
                               & (df['股票代码'] == stock)]['收盘价'].values[0]
                holding_days += 1
                if fut_price / buy_price >= 1.03:  # 如果涨幅超过3%，则平仓
                    sell_price = fut_price  # 卖出价格为当天收盘价
                    cash_balance += sell_price / buy_price * \
                        cash_balance - fees * cash_balance  # 更新资金余额
                    new_row = {'日期': date, '交易标的': stock, '买入价': buy_price, '卖出价': sell_price,
                               '持仓周期': holding_days, '收益率': sell_price / buy_price - 1}
                    all_trading_detail = all_trading_detail.append(
                        new_row, ignore_index=True)  # 记录交易细节
                    break
    elif ema121_ratio_product < 0.9:  # 熊市策略
        for stock in group['股票代码']:  # 做空所有当天的股票
            cash_balance -= fees * cash_balance  # 扣除手续费
            sell_price = group[group['股票代码'] ==
                               stock]['收盘价'].values[0]  # 卖出价格为当天收盘价
            holding_days = 1  # 已持有天数
            for i in range(1, holding_period+1):
                # 如果未来函数中没有数据了，则退出持仓
                if date + pd.Timedelta(days=i) not in df['日期'].unique():
                    break
                fut_price = df[(df['日期'] == date + pd.Timedelta(days=i))
                               & (df['股票代码'] == stock)]['收盘价'].values[0]
                holding_days += 1
                if fut_price / sell_price <= 0.97:  # 如果跌幅超过3%，则平仓
                    buy_price = fut_price  # 买入价格为当天收盘价
                    cash_balance -= fees * cash_balance  # 再扣除一次手续费
                    cash_balance += 2 * sell_price / buy_price * cash_balance - \
                        fees * cash_balance  # 更新资金余额，同时还要偿还卖空借入的股票
                    new_row = {'日期': date, '交易标的': stock, '卖出价': sell_price, '买入价': buy_price,
                               '持仓周期': holding_days, '收益率': buy_price / sell_price - 1}
                    all_trading_detail = all_trading_detail.append(
                        new_row, ignore_index=True)  # 记录交易细节
                    break
    else:  # 震荡策略
        for stock in group['股票代码']:  # 买入所有当天的股票
            cash_balance -= fees * cash_balance  # 扣除手续费
            buy_price = group[group['股票代码'] ==
                              stock]['收盘价'].values[0]  # 买入价格为当天收盘价
            holding_days = 1  # 已持有天数
            for i in range(1, holding_period+1):
                # 如果未来函数中没有数据了，则退出持仓
                if date + pd.Timedelta(days=i) not in df['日期'].unique():
                    break
                fut_price = df[(df['日期'] == date + pd.Timedelta(days=i))
                               & (df['股票代码'] == stock)]['收盘价'].values[0]
                holding_days += 1
                if fut_price / buy_price >= 1.03 or fut_price / buy_price <= 0.97:  # 如果涨幅超过3%或跌幅超过3%，则平仓
                    sell_price = fut_price  # 卖出价格为当天收盘价
                    cash_balance += sell_price / buy_price * \
                        cash_balance - fees * cash_balance  # 更新资金余额
                    new_row = {'日期': date, '交易标的': stock, '买入价': buy_price, '卖出价': sell_price,
                               '持仓周期': holding_days, '收益率': sell_price / buy_price - 1}
                    all_trading_detail = all_trading_detail.append(
                        new_row, ignore_index=True)  # 记录交易细节
                    break
    daily_cash_balance[date] = cash_balance  # 存储当日资金余额

# 计算每日收益率和净资产走势
all_daily_return['日期'] = sorted(daily_cash_balance.keys())
all_daily_return['收益率'] = all_daily_return['日期'].apply(lambda x: 0 if x == all_daily_return['日期'].min() else
                                                       daily_cash_balance[x] / daily_cash_balance[all_daily_return['日期'][all_daily_return['日期'] < x].max()] - 1)
all_daily_return['净资产收益率'] = all_daily_return['日期'].apply(
    lambda x: daily_cash_balance[x] / cash_balance - 1)

# 输出交易细节、每日收益率和净资产走势到csv文件
all_trading_detail.to_csv(f'{name}交易细节.csv', index=False)
all_daily_return.to_csv(f'{name}收益率和净资产收益率.csv', index=False)
