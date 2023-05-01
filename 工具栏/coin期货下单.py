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
# 发生bug: APIError(code=-4131): The counterparty's best price does not meet the PERCENT_PRICE filter limit.
# 针对波场的-4131是市场交易量过小时下单可能导致市场偏离的市场保护机制
# 卖出和买入的价格是0，说明订单尚未成交

# 针对期货市场的精度设置
# precision = int(symbol_info['quantityPrecision'])
# print(symbol, '数量精度', precision)
# price_precision = int(symbol_info['pricePrecision'])
# print(symbol, '价格精度', price_precision)

# 针对现货市场的精度设置
# price_precision = int(symbol_info['quotePrecision'])
# print(symbol, '价格精度', price_precision)
# precision = int(symbol_info['quoteAssetPrecision'])
# print(symbol, '报价资产数量精度', precision)
# precisionbase = int(symbol_info['baseAssetPrecision'])
# print(symbol, '基础资产数量精度', precision)
# 基础资产的价格和交易量限制

# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)

# # 币安的api配置(现货测试网)
# api_key = "xxnNHT4HKiOVVhR7E3KvL77aVaZgiH5xId8IQGhoZX6DNUUMBluAfl8XiE8WRdEh"
# api_secret = "ydYnPLRl2R2EUc2fH7GViSLU1ZMs4aciQb2yI6d3BqbLohghE2L8C4Usc54qKMHk"
# # 创建Binance客户端
# client = Client(api_key, api_secret, testnet=True)

# 币安的api配置(期货测试网)
api_key = "266950dec031270d32fed06a552c2698cf662f0e32c4788acf25646bba7ef2c6"
api_secret = "1a2b9793419db20e99a307be8ac04fec7a43bd77d46b71b161456105161b164d"
# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True,
                base_endpoint='https://testnet.binancefuture.com/fapi')

# 连接MongoDB数据库
dbclient = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = dbclient["wth000"]
name = "BTC"
# collection = db[f'{name}']
# # 获取数据并转换为DataFrame格式
# data = pd.DataFrame(list(collection.find()))

collection_write_order = db[f'{name}期货order']
collection_write_sell = db[f'{name}期货sell']

symbol_dict = client.futures_exchange_info()

balances = client.futures_account_balance()  # 获取永续合约账户资产余额
# 获取账户余额
for balance in balances:
    if balance['asset'] == 'USDT':
        usdt_balance = balance['balance']
print('USDT余额：', usdt_balance)


def buy():
    # 获取计划交易的标的
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'TRXUSDT']
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
            # 首先需要找到包含目标交易对数据的那个子字典
            for symbol_info in symbol_dict['symbols']:
                if symbol_info['symbol'] == f'{str(symbol)}':
                    symbol_info = symbol_info
                    break
            # 针对期货市场的精度设置
            # precision = int(symbol_info['quantityPrecision'])
            # print(symbol, '数量精度', precision)
            # price_precision = int(symbol_info['pricePrecision'])
            # print(symbol, '价格精度', price_precision)

            # {'stepSize': '0.001', 'filterType': 'LOT_SIZE', 'maxQty': '1000', 'minQty': '0.001'}：数量限制条件，包括最小数量、最大数量、数量步长等信息。
            # 最小订单精度
            min_order_value = symbol_info['filters'][1]['minQty']
            min_order_precision = abs(int(math.log10(float(min_order_value))))
            print(f'{symbol}永续合约最小下单精度：{min_order_precision}')
            # 实时获取当前卖一和卖二价格
            depth = client.futures_order_book(symbol=symbol, limit=5)
            ask_price_1 = float(depth['asks'][0][0])
            ask_price_2 = float(depth['asks'][1][0])
            bid_price_1 = float(depth['bids'][0][0])
            bid_price_2 = float(depth['bids'][1][0])
            print(depth)
            # 计算买卖均价
            target_price = (ask_price_1+bid_price_1)/2
            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if bid_price_1/bid_price_2 >= 0.99 and ask_price_2/ask_price_1 <= 1.01:
                # 下单
                symbol = str(symbol)
                quantity = round(15/target_price, min_order_precision)

                order = client.futures_create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=quantity
                )
                print("下单信息：", order)

                collection_write_order.insert_one({
                    'orderId': order['orderId'],
                    'time': int(time.time()),
                    'symbol': symbol,
                    'quantity': quantity,
                    'buy_price': float(order['avgPrice']),
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
    try:
        # 查询所有待卖的订单
        orders = collection_write_order.find({
            'status': 'pending',
            'sell_time': {'$lt': int(time.time())}
        })

        for order in orders:
            print("检查未成交订单:", order)
            # 下单继续卖出
            sell_order = client.futures_create_order(
                symbol=order['symbol'],
                side=Client.SIDE_SELL,
                type=Client.ORDER_TYPE_MARKET,
                quantity=order['quantity']
            )

            if sell_order.get('avgPrice'):
                # 更新订单为完成，并插入数据
                collection_write_order.update_one(
                    {'orderId': order['orderId']},
                    {'$set': {
                        'sell_time': int(time.time()),
                        'sell_price': float(sell_order['avgPrice']),
                        'status': 'done'
                    }}
                )

                collection_write_sell.insert_one({
                    'orderId': sell_order['orderId'],
                    'time': int(time.time()),
                    'symbol': order['symbol'],
                    'quantity': order['quantity'],
                    'buy_price': order['buy_price'],
                    'sell_price': float(sell_order['avgPrice'])
                })
                print("已卖出信息：", {
                    'orderId': sell_order['orderId'],
                    'time': int(time.time()),
                    'symbol': order['symbol'],
                    'quantity': order['quantity'],
                    'buy_price': order['buy_price'],
                    'sell_price': float(sell_order['avgPrice'])
                })

                # 删除已下单信息
                collection_write_order.delete_one(
                    {'orderId': order['orderId']})
            else:
                print("无法卖出订单:", order)
    # 函数代码
    except Exception as e:
        print(f"发生异常：{e}")


def sell():
    try:
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
                sell_order = client.futures_create_order(
                    symbol=order['symbol'],
                    side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=order['quantity']
                )
                print("卖出信息：", sell_order)

                if sell_order.get('avgPrice'):
                    # 更新订单信息
                    collection_write_order.update_one(
                        {'orderId': order['orderId']},
                        {'$set': {
                            'sell_time': int(time.time()),
                            'sell_price': float(sell_order['avgPrice']),
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
                        'sell_price': float(sell_order['avgPrice'])
                    })
                    print("已卖出信息：", {
                        'orderId': sell_order['orderId'],
                        'time': int(time.time()),
                        'symbol': order['symbol'],
                        'quantity': order['quantity'],
                        'buy_price': order['buy_price'],
                        'sell_price': float(sell_order['avgPrice'])
                    })

                    # 删除已下单信息
                    collection_write_order.delete_one(
                        {'orderId': order['orderId']})
                else:
                    print("无法卖出订单:", order)
    # 函数代码
    except Exception as e:
        print(f"发生异常：{e}")


buy()
sell()

# 每5分钟执行一次买入操作
schedule.every(60).minutes.do(buy)

# 每分钟执行一次卖出操作
schedule.every(60).minutes.do(sell)

while True:
    schedule.run_pending()
    time.sleep(1)
