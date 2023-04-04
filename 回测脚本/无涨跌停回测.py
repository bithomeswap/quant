import pandas as pd
import numpy as np
from pymongo import MongoClient
import datetime

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
etf_collection = db['stock指标']
mairu_collection = db['无涨跌停回测']
# 买卖手续费各设置成千分之二


def backtest(code_df):
    buy_dates = []
    sell_dates = []
    buy_prices = []
    sell_prices = []
    buy_volumes = []
    sell_volumes = []
    total_costs = []
    profit_rates = []
    profit_all = []

    # 买入信号
    try:
        # 前面这些限制的目的是避免索引越界
        for i in range(2, len(code_df)-1):
            current_data = code_df.iloc[i]
            previous_data = code_df.iloc[i-1]
            next_data = code_df.iloc[i+1]
            # 买入信号
            # 前一天涨跌幅的平方达到0.25
            if previous_data['涨跌幅']*previous_data['涨跌幅'] > 0.25:
                # 当日开盘价低于前一日最高价、且高于前一日最低价
                if current_data['开盘'] < previous_data['最高'] and current_data['开盘'] > previous_data['最低']:
                    # MACD金叉
                    if previous_data['MACD'] < 0 and current_data['MACD'] > 0:
                        # 产生买入信号
                        buy_volume = round(previous_data['成交量'] * 0.001, 0)
                        buy_price = current_data['收盘']
                        buy_cost = round((buy_price * buy_volume) * 0.002, 2)

                        buy_dates.append(current_data['日期'])
                        buy_prices.append(buy_price)
                        buy_volumes.append(buy_volume)

                        sell_volume = buy_volume
                        sell_price = next_data['收盘']
                        sell_cost = round(
                            (sell_price * sell_volume) * 0.002, 2)

                        sell_dates.append(next_data['日期'])
                        sell_prices.append(sell_price)
                        sell_volumes.append(sell_volume)

                        total_cost = sell_cost+buy_cost
                        total_costs.append(total_cost)

                        profit_all.append(
                            round((sell_price * sell_volume - buy_price * buy_volume -
                                   (sell_price * sell_volume) * 0.002-(buy_price * buy_volume) * 0.002)))

                        profit_rates.append(
                            round((sell_price * sell_volume - buy_price * buy_volume -
                                   (sell_price * sell_volume) * 0.002-(buy_price * buy_volume) * 0.002) / (buy_price * buy_volume), 10))

    except Exception as e:
        print(f"回测{code_df.iloc[0]['代码']}时发生错误：{e}")

    # 将买卖交易信息写入数据库
    if buy_dates:
        mairu_df = pd.DataFrame({
            "代码": [code_df.iloc[0]['代码']] * len(buy_dates),
            "日期": buy_dates,
            "买入价格": buy_prices,
            "买入量": buy_volumes,
            "卖出日期": sell_dates,
            "卖出价格": sell_prices,
            "卖出量": sell_volume,
            "交易总磨损": total_costs,
            "收益额": profit_all,
            "收益率": profit_rates
        })
        mairu_collection.insert_many(mairu_df.to_dict("records"))


# 获取所有代码
codes = etf_collection.distinct('代码')
for code in codes:
    # 获取每一只ETF的数据，并根据日期升序排序
    code_df = pd.DataFrame(
        list(etf_collection.find({'代码': code}))).sort_values(by='日期')
    # 进行回测
    backtest(code_df)
