import os
import talib
import pandas as pd
import requests
import math
import datetime
import pytz
import time
import schedule
from binance.client import Client
from pymongo import MongoClient


# 函数说明
# FuturesMarketDataClient：期货市场数据的客户端，其中包含了get_contract_klines、get_recent_trades、get_historical_trades等函数，可以获取期货相关的K线数据、最近成交记录和历史成交记录。
# FuturesAccountClient：期货账户信息的客户端，其中包含了get_account、get_position_information、get_order等函数，可以查询期货账户余额、持仓信息，以及查询、取消、下单等操作。
# FuturesMarketClient：期货市场交易的客户端，其中包含了create_order、cancel_order、batch_create_orders等函数，可用于期货合约的下单、撤单等操作。
# FuturesUserDataClient：期货用户数据的客户端，其中包含了subscribe_user_data_event、unsubscribe_user_data_event等函数，可以订阅和取消订阅期货用户数据事件，如持仓更新数据、成交更新数据等。


# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)
# 币安的api配置(测试网)
api_key = "xxnNHT4HKiOVVhR7E3KvL77aVaZgiH5xId8IQGhoZX6DNUUMBluAfl8XiE8WRdEh"
api_secret = "ydYnPLRl2R2EUc2fH7GViSLU1ZMs4aciQb2yI6d3BqbLohghE2L8C4Usc54qKMHk"

# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True,
                base_endpoint='https://testnet.binancefuture.com/fapi')

# 连接MongoDB数据库
dbclient = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = dbclient["wth000"]
name = "BTCUSDT"

collection_write_order = db[f'{name}期货order']
collection_write_sell = db[f'{name}期货sell']

symbol_dict = client.futures_exchange_info()


account = client.get_account()
# account = client.futures_account_balance() # 获取持仓余额
# 获取账户余额
# for asset in account['assets']:
#     if asset['asset'] == 'USDT':
#         usdt_balance = float(asset['balance'])
#         print('USDT余额：', usdt_balance)
print(account)


def buy():
    # 获取计划交易的标的
    symbols = ['BTCUSDT', 'ETHUSDT']
    print(symbols)
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        # try:
        # 首先需要找到包含目标交易对数据的那个子字典
        for symbol_info in symbol_dict['symbols']:
            if symbol_info['symbol'] == f'{str(symbol)}':
                symbol_info = symbol_info
                break
        # 'pricePrecision': 2：交易对价格小数点位数。
        # 'quantityPrecision': 3：交易对数量小数点位数。
        precision = int(symbol_info['quantityPrecision'])
        price_precision = int(symbol_info['pricePrecision'])
        print(symbol, '下单精度', precision)
        # {'stepSize': '0.001', 'filterType': 'LOT_SIZE', 'maxQty': '1000', 'minQty': '0.001'}：数量限制条件，包括最小数量、最大数量、数量步长等信息。
        # 最小订单精度
        min_order_value = symbol_info['filters'][1]['minQty']
        min_order_precision = abs(int(math.log10(float(min_order_value))))
        print(f'{symbol}永续合约最小下单精度：{min_order_precision}')
#             # 实时获取当前卖一和卖二价格
#             depth = client.futures_order_book(symbol=symbol, limit=5)
#             ask_price_1 = float(depth['asks'][0][0])
#             ask_price_2 = float(depth['asks'][1][0])
#             print(depth)
#             # 计算预定价格
#             target_price = ask_price_2 * 1.001
#             # 判断当前卖一不高于预定价格，卖二卖一差距较小
#             if ask_price_1 <= target_price and ask_price_2/ask_price_1 <= 1.01:
#                 # 下单
#                 symbol = str(symbol)
#                 quantity = round(11/target_price, min_order_precision)
#                 order = client.futures_create_order(
#                     symbol=symbol,
#                     side=Client.SIDE_BUY,
#                     type=Client.ORDER_TYPE_MARKET,
#                     quantity=quantity
#                 )
#                 print("下单信息：", order)

#                 collection_write_order.insert_one({
#                     'orderId': order['orderId'],
#                     'time': int(time.time()),
#                     'symbol': symbol,
#                     'quantity': quantity,
#                     'buy_price': float(order['avgPrice']),
#                     'sell_time': None,
#                     'sell_price': None,
#                     'status': 'pending'
#                 })
#         except Exception as e:
#             # 错误次数加1，并输出错误信息
#             error_count += 1
#             print(f"发生bug: {e}")
#             continue

#     # 输出异常次数
#     print(f"共出现{error_count}次异常")


# # def check_pending_order():
# #     # 查询所有待卖的订单
# #     orders = collection_write_order.find({
# #         'status': 'pending',
# #         'sell_time': {'$lt': int(time.time())}
# #     })

# #     for order in orders:
# #         print("检查未成交订单:", order)
# #         # 下单继续卖出
# #         sell_order = client.futures_create_order(
# #             symbol=order['symbol'],
# #             side=Client.SIDE_SELL,
# #             type=Client.ORDER_TYPE_MARKET,
# #             quantity=order['quantity']
# #         )

# #         if sell_order.get('avgPrice'):
# #             # 更新订单为完成，并插入数据
# #             collection_write_order.update_one(
# #                 {'orderId': order['orderId']},
# #                 {'$set': {
# #                     'sell_time': int(time.time()),
# #                     'sell_price': float(sell_order['avgPrice']),
# #                     'status': 'done'
# #                 }}
# #             )

# #             collection_write_sell.insert_one({
# #                 'orderId': sell_order['orderId'],
# #                 'time': int(time.time()),
# #                 'symbol': order['symbol'],
# #                 'quantity': order['quantity'],
# #                 'buy_price': order['buy_price'],
# #                 'sell_price': float(sell_order['avgPrice'])
# #             })
# #             print("已卖出信息：", {
# #                 'orderId': sell_order['orderId'],
# #                 'time': int(time.time()),
# #                 'symbol': order['symbol'],
# #                 'quantity': order['quantity'],
# #                 'buy_price': order['buy_price'],
# #                 'sell_price': float(sell_order['avgPrice'])
# #             })

# #             # 删除已下单信息
# #             collection_write_order.delete_one({'orderId': order['orderId']})
# #         else:
# #             print("无法卖出订单:", order)


# # def sell():
# #     # 首先检查未成交的订单
# #     check_pending_order()
# #     # 查询已下单且未卖出的订单
# #     orders = collection_write_order.find({
# #         'status': 'pending',
# #         'sell_time': None
# #     })
# #     for order in orders:
# #         # 计算卖出时间
# #         sell_time = order['time'] + 86400
# #         # sell_time = order['time']
# #         # 如果卖出时间已经到了，就执行卖出操作
# #         if int(time.time()) >= sell_time:
# #             # 卖出订单
# #             sell_order = client.futures_create_order(
# #                 symbol=order['symbol'],
# #                 side=Client.SIDE_SELL,
# #                 type=Client.ORDER_TYPE_MARKET,
# #                 quantity=order['quantity']
# #             )
# #             print("卖出信息：", sell_order)

# #             if sell_order.get('avgPrice'):
# #                 # 更新订单信息
# #                 collection_write_order.update_one(
# #                     {'orderId': order['orderId']},
# #                     {'$set': {
# #                         'sell_time': int(time.time()),
# #                         'sell_price': float(sell_order['avgPrice']),
# #                         'status': 'done'
# #                     }}
# #                 )

# #                 # 插入卖出订单信息
# #                 collection_write_sell.insert_one({
# #                     'orderId': sell_order['orderId'],
# #                     'time': int(time.time()),
# #                     'symbol': order['symbol'],
# #                     'quantity': order['quantity'],
# #                     'buy_price': order['buy_price'],
# #                     'sell_price': float(sell_order['avgPrice'])
# #                 })
# #                 print("已卖出信息：", {
# #                     'orderId': sell_order['orderId'],
# #                     'time': int(time.time()),
# #                     'symbol': order['symbol'],
# #                     'quantity': order['quantity'],
# #                     'buy_price': order['buy_price'],
# #                     'sell_price': float(sell_order['avgPrice'])
# #                 })

# #                 # 删除已下单信息
# #                 collection_write_order.delete_one(
# #                     {'orderId': order['orderId']})
# #             else:
# #                 print("无法卖出订单:", order)


# # # 每5分钟执行一次买入操作
# # schedule.every(5).minutes.do(buy)

# # # 每分钟执行一次卖出操作
# # schedule.every(1).minutes.do(sell)

# # while True:
# #     schedule.run_pending()
# #     time.sleep(1)

# buy()
