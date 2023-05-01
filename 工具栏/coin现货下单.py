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
# get_exchange_info()：获取当前交易对信息，包括交易对精度、限价单最大值、挂单最小值等。
# get_order_book(symbol, limit=100)：获取指定交易对的深度信息，包括卖五、买五等信息。
# get_recent_trades(symbol, limit=500)：获取指定交易对最近成交的交易记录，最多返回500条。
# get_historical_trades(symbol, limit=500, fromId=None)：获取指定交易对的历史成交记录，最多返回500条，可以指定起始成交记录ID。
# get_klines(symbol, interval, limit=500, startTime=None, endTime=None)：获取指定交易对和时间区间内的K线数据，包括开盘价、收盘价、最高价、最低价等。
# get_avg_price(symbol)：获取指定交易对最新的平均价格。
# get_ticker(symbol)：获取指定交易对最新的价格信息，包括最新成交价、成交量等。
# get_all_tickers()：获取当前所有交易对的最新价格信息。
# get_account(recvWindow=60000)：获取当前账户信息，包括余额、冻结资金等。
# get_open_orders(symbol=None, recvWindow=60000)：获取当前账户的所有未成交的订单信息，可以指定交易对。
# create_test_order(symbol, side, type, timeInForce, quantity, price=None, stopPrice=None, icebergQty=None, newClientOrderId=None, recvWindow=60000)：下单测试接口，用于测试订单参数是否正确。
# create_order(symbol, side, type, timeInForce, quantity, price=None, stopPrice=None, icebergQty=None, newClientOrderId=None, recvWindow=60000)：下单接口，用于在指定交易对下单。
# cancel_order(symbol, orderId=None, origClientOrderId=None, newClientOrderId=None, recvWindow=60000)：取消指定订单，可以通过订单ID或客户端自定义ID来取消。
# get_order(symbol, orderId=None, origClientOrderId=None, recvWindow=60000)：获取指定订单的信息，可以通过订单ID或客户端自定义ID来查询。
# get_all_orders(symbol, orderId=None, startTime=None, endTime=None, limit=500, recvWindow=60000)：获取当前账户在指定交易对下的所有订单信息。


# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)
# 币安的api配置(测试网)
api_key = "xxnNHT4HKiOVVhR7E3KvL77aVaZgiH5xId8IQGhoZX6DNUUMBluAfl8XiE8WRdEh"
api_secret = "ydYnPLRl2R2EUc2fH7GViSLU1ZMs4aciQb2yI6d3BqbLohghE2L8C4Usc54qKMHk"

# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True)

# 连接MongoDB数据库
dbclient = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = dbclient["wth000"]
name = "BTC"
# collection = db[f'{name}']
# # 获取数据并转换为DataFrame格式
# data = pd.DataFrame(list(collection.find()))
# print("数据读取成功")
collection_write_order = db[f'{name}order']
collection_write_sell = db[f'{name}sell']


# def sell_all():市价卖出所有代币
#     # 获取账户余额
#     balances = client.get_account()['balances']
#     for balance in balances:
#         asset = balance['asset']
#         free_balance = float(balance['free'])
#         locked_balance = float(balance['locked'])
#         total_balance = free_balance + locked_balance
#         if asset != 'USDT' and total_balance > 0:
#             symbol = asset + 'USDT'
#             # 执行市价卖单
#             client.order_market_sell(
#                 symbol=symbol,
#                 quantity=total_balance
#             )
#             print(f"卖出{asset}成功！")
# sell_all()

def buy():
    # 获取账户余额
    balances = client.get_account()['balances']
    print(balances)
    for balance in balances:
        if balance['asset'] == 'USDT':
            usdt_balance = float(balance['free'])
            print('USDT余额：', usdt_balance)
    # 获取计划交易的标的
    symbols = ['BTCUSDT', 'ETHUSDT']
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
            symbol_info = client.get_symbol_info(symbol)
            # 'pricePrecision': 2：交易对价格小数点位数。
            # 'quantityPrecision': 3：交易对数量小数点位数。
            precision = int(symbol_info['quantityPrecision'])  # 获取数量限制
            price_precision = int(symbol_info['pricePrecision'])  # 获取价格限制
            print(symbol, '下单精度', precision)
            # {'stepSize': '0.001', 'filterType': 'LOT_SIZE', 'maxQty': '1000', 'minQty': '0.001'}：数量限制条件，包括最小数量、最大数量、数量步长等信息。
            # 最小订单精度
            min_order_value = symbol_info['filters'][1]['minQty']
            min_order_precision = abs(int(math.log10(float(min_order_value))))
            print(f"{symbol}最小订单精度: {min_order_precision}")
            # 实时获取当前卖一和卖二价格
            depth = client.get_order_book(symbol=symbol, limit=5)
            ask_price_1 = float(depth['asks'][0][0])
            ask_price_2 = float(depth['asks'][1][0])
            print(depth)
            # 计算预定价格
            target_price = ask_price_2 * 1.001
            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if ask_price_1 <= target_price and ask_price_2/ask_price_1 <= 1.01:
                # 下单
                symbol = str(symbol)
                quantity = round(11/target_price, min_order_precision)
                order = client.order_market_buy(
                    symbol=symbol,
                    quantity=quantity
                )
                print("下单信息：", order)

                collection_write_order.insert_one({
                    'orderId': order['orderId'],
                    'time': int(time.time()),
                    'symbol': symbol,
                    'quantity': quantity,
                    'buy_price': float(order['fills'][0]['price']),
                    'sell_time': None,
                    'sell_price': None,
                    'status': 'pending'
                })
        except Exception as e:
            # 错误次数加1，并输出错误信息
            error_count += 1
            print(f"发生bug: {e}")
            continue

    # 输出异常次数
    print(f"共出现{error_count}次异常")


def check_pending_order():
    # 查询所有待卖的订单
    orders = collection_write_order.find({
        'status': 'pending',
        'sell_time': {'$lt': int(time.time())}
    })

    for order in orders:
        print("检查未成交订单:", order)
        # 下单继续卖出
        sell_order = client.order_market_sell(
            symbol=order['symbol'],
            quantity=order['quantity']
        )

        if sell_order.get('fills'):
            # 更新订单为完成，并插入数据
            collection_write_order.update_one(
                {'orderId': order['orderId']},
                {'$set': {
                    'sell_time': int(time.time()),
                    'sell_price': float(sell_order['fills'][0]['price']),
                    'status': 'done'
                }}
            )

            collection_write_sell.insert_one({
                'orderId': sell_order['orderId'],
                'time': int(time.time()),
                'symbol': order['symbol'],
                'quantity': order['quantity'],
                'buy_price': order['buy_price'],
                'sell_price': float(sell_order['fills'][0]['price'])
            })
            print("已卖出信息：", {
                'orderId': sell_order['orderId'],
                'time': int(time.time()),
                'symbol': order['symbol'],
                'quantity': order['quantity'],
                'buy_price': order['buy_price'],
                'sell_price': float(sell_order['fills'][0]['price'])
            })

            # 删除已下单信息
            collection_write_order.delete_one({'orderId': order['orderId']})
        else:
            print("无法卖出订单:", order)


def sell():
    # 首先检查未成交的订单
    check_pending_order()
    # 查询已下单且未卖出的订单
    orders = collection_write_order.find({
        'status': 'pending',
        'sell_time': None
    })
    for order in orders:
        # 计算卖出时间
        sell_time = order['time'] + 86400
        # sell_time = order['time']
        # 如果卖出时间已经到了，就执行卖出操作
        if int(time.time()) >= sell_time:
            # 卖出订单
            sell_order = client.order_market_sell(
                symbol=order['symbol'],
                quantity=order['quantity']
            )
            print("卖出信息：", sell_order)

            if sell_order.get('fills'):
                # 更新订单信息
                collection_write_order.update_one(
                    {'orderId': order['orderId']},
                    {'$set': {
                        'sell_time': int(time.time()),
                        'sell_price': float(sell_order['fills'][0]['price']),
                        'status': 'done'
                    }}
                )

                # 插入卖出订单信息
                collection_write_sell.insert_one({
                    'orderId': sell_order['orderId'],
                    'time': int(time.time()),
                    'symbol': order['symbol'],
                    'quantity': order['quantity'],
                    'buy_price': order['buy_price'],
                    'sell_price': float(sell_order['fills'][0]['price'])
                })
                print("已卖出信息：", {
                    'orderId': sell_order['orderId'],
                    'time': int(time.time()),
                    'symbol': order['symbol'],
                    'quantity': order['quantity'],
                    'buy_price': order['buy_price'],
                    'sell_price': float(sell_order['fills'][0]['price'])
                })

                # 删除已下单信息
                collection_write_order.delete_one(
                    {'orderId': order['orderId']})
            else:
                print("无法卖出订单:", order)


# # 每5分钟执行一次买入操作
# schedule.every(60).minutes.do(buy)

# # 每分钟执行一次卖出操作
# schedule.every(60).minutes.do(sell)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

buy()
sell()
