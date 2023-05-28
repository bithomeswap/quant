import pandas as pd
import numpy as np

def trading_algorithm(df, cash_balance, tick_size, min_qty, tax, fee, holding_period):
    '''
    交易算法函数，根据持仓周期内的涨跌幅进行交易，最终返回收益率。
    :param df: 持仓周期内的行情数据（pandas.DataFrame）
    :param cash_balance: 可用资金（float）
    :param tick_size: 最小价格变动单位（float）
    :param min_qty: 最小交易数量（int）
    :param tax: 印花税率（float）
    :param fee: 手续费率（float）
    :param holding_period: 每个标的的持仓周期（int）
    :return: 收益率，持仓股票数量，持仓时间，成本价，卖出价（float或pandas.Series）
    '''
    # 计算真实的持仓
    position = cash_balance / len(df)
    available_cash = position * (1 - tax)
    # 计算每只标的的可用资金
    df['available_cash'] = available_cash
    
    # 根据购买日收盘价加上0.01元作为真实的购买价格
    df['buy_price'] = df['close_price'] + 0.01
    # 最小下单金额
    min_amount = tick_size * min_qty
    # 无法买入资金
    df['no_buy_cash'] = df['available_cash'] % min_amount
    # 真实的下单金额
    df['real_cash'] = df['available_cash'] - df['no_buy_cash']
    
    # 初始化交易结果
    buy_prices = pd.Series(index=df.index)
    sell_prices = pd.Series(index=df.index)
    holding_times = pd.Series(index=df.index)
    holding_qty = pd.Series(index=df.index)
    holding_cost = pd.Series(index=df.index)
    sell_signals = pd.Series(index=df.index)
    buy_signals = pd.Series(index=df.index)

    # 交易开始
    for i in range(holding_period, len(df)):
        # 买入信号
        if df['close_price'][i] < df['close_price'][i - holding_period]:
            buy_signals[i] = True
            # 买入数量向下取整
            qty = np.floor(df['real_cash'][i] / df['buy_price'][i] / min_qty) * min_qty
            # 当前成本价
            cost = df['real_cash'][i] / qty
            # 累计持仓数量
            holding_qty[i] = qty
            # 累计持仓时间
            holding_times[i] = holding_period
            # 累计成本价
            holding_cost[i] = cost
            # 扣除手续费
            cash_balance -= qty * cost * fee + qty * cost * tax
            # 计算持仓股票数量
            holding_qty[i] = qty
            
        # 卖出信号
        elif df['close_price'][i] > df['close_price'][i - holding_period]:
            sell_signals[i] = True
            # 计算卖出数量
            qty = holding_qty[i - holding_period]
            # 计算卖出价
            sell_price = df['close_price'][i] - 0.01
            # 记录买入价格和卖出价格
            buy_prices[i] = holding_cost[i - holding_period]
            sell_prices[i] = sell_price
            # 计算收益率
            rate = (sell_price - holding_cost[i - holding_period]) / holding_cost[i - holding_period]
            # 计算收益金额
            profit = qty * sell_price * (1 - tax) - qty * holding_cost[i - holding_period] * (1 + fee + tax)
            # 更新可用资金
            cash_balance += profit
            # 清空持仓数量、持仓时间和成本价
            holding_qty[i] = np.nan
            holding_times[i] = np.nan
            holding_cost[i] = np.nan

    # 结束交易，计算收益率
    sell_signals.iloc[-1] = True
    sell_prices.iloc[-1] = df['close_price'].iloc[-1] - 0.01
    buy_prices.ffill(inplace=True)
    holding_times[buy_signals.index[0]] = holding_period
    holding_times.ffill(inplace=True)
    holding_qty[buy_signals.index[0]] = cash_balance / df['buy_price'][buy_signals.index[0]]
    holding_qty.ffill(inplace=True)
    holding_cost[buy_signals.index[0]] = df['buy_price'][buy_signals.index[0]]
    holding_cost.fillna(method='ffill', inplace=True)
    holding_profit = holding_qty * (sell_prices - holding_cost) * (1 - fee - tax)
    holding_profit.fillna(0, inplace=True)
    holding_profit = holding_profit.cumsum()
    holding_profit += cash_balance - np.sum(holding_qty * holding_cost) * (1 + fee + tax)

    return holding_profit.iloc[-1] / np.sum(holding_qty * holding_cost), np.sum(holding_qty), np.sum(holding_times), holding_cost.mean(), sell_prices.mean()

# 测试
df = pd.read_csv('data.csv')
cash_balance = 10000
tick_size = 0.01
min_qty = 100
tax = 0.001
fee = 0.0005
holding_period = 30

result = trading_algorithm(df, cash_balance, tick_size, min_qty, tax, fee, holding_period)
print(result)
