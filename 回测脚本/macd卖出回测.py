import pandas as pd
from pymongo import MongoClient
import datetime

# 链接数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
collection = db['stock指标']
mairu_collection = db['mairu']
maichu_collection = db['maichu1']


# 遍历每一只股票的买入信息
for mairu in mairu_collection.find():
    stock_code = mairu['代码']
    buy_date = mairu['日期']
    buy_price = mairu['买入价格']
    buy_volume = mairu['买入量']
    buy_cost = mairu['买入总磨损']
    # 根据买入日期查找卖出日期，并计算收益率
    code_df = pd.DataFrame(
        list(collection.find({'代码': stock_code}).sort('日期')))

    buy_index = code_df[code_df['日期'] == buy_date].index[0]

    buy_sell = buy_price        # 之前的买入模型中buy_price为买入前一天的收盘价所以这里根这比较就好了
    # 判断循环中是否发生过买入行为
    action = 0
    for i in range(buy_index+1, len(code_df)-1):
        sell_date = code_df.iloc[i]['日期']
        sell_open = code_df.iloc[i]['开盘']
        sell_close = code_df.iloc[i]['收盘']
        sell_high = code_df.iloc[i]['最高']
        sell_low = code_df.iloc[i]['最低']
        sell_volume = code_df.iloc[i]['成交量']

        print((sell_open-buy_sell)/buy_sell)

        # 满足卖出条件
        if -0.08 < ((sell_open-buy_sell)/buy_sell):
            # 计算卖出信息
            sell_price = sell_close
            sell_cost = round((sell_price * buy_volume) * 0.002, 10)
            sell_profit = round((((sell_price * buy_volume) - (buy_price * buy_volume) - buy_cost - sell_cost) /
                                ((buy_price * buy_volume) + buy_cost)), 10)
            profit_all = round(
                ((sell_price * buy_volume) - (buy_price * buy_volume) - buy_cost - sell_cost), 10)

            # 写入卖出信息到数据库
            maichu_collection.insert_one({
                '代码': stock_code,
                '买入日期': buy_date,
                '买入价格': buy_price,
                '买入量': buy_volume,
                '买入总磨损': buy_cost,
                '卖出日期': sell_date,
                '卖出价格': sell_price,
                '卖出量': buy_volume,
                '卖出总磨损': sell_cost,
                '总收益': profit_all,
                '收益率': sell_profit
            })
            # 跳出内层循环，进行下一次买入操作
            action = 1
            break
        # 更新比较价格为前一天的收盘价
        buy_sell = sell_close

    if action == 0:
        maichu_collection.insert_one({
            '代码': stock_code,
            '买入日期': buy_date,
            '买入价格': buy_price,
            '买入量': buy_volume,
            '买入总磨损': buy_cost,
            '卖出日期':  None,
            '卖出价格':  None,
            '卖出量': None,
            '卖出总磨损':  None,
            '总收益':  None,
            '收益率': None
        })
