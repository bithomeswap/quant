import os
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

# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)

# # 币安的api配置(现货测试网)
# api_key = "ERqD8OaGhyPEhZciJs8XZgy7d83x1cyF7U3s2TYw0wZR2SCZMNmf8MzZ8TYzuIzT"
# api_secret = "CF2XpFnKKrasCiyZ38D3duMLqFseanJ6iRbmXmrDvqNFlIWJXZbhrlvha5kGxFjr"
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
name = "COIN"
collection_write = db[f'order期货{name}']
# 获取计划交易的标的
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'TRXUSDT']
symbol_dict = client.futures_exchange_info()

# def sell_all():  # 市价平仓所有持仓
#     for symbol in symbols:    # 获取当前持仓信息
#         position = client.futures_position_information(symbol=symbol)
#         # 遍历所有持仓并按相反方向下单平仓
#         for p in position:
#             if float(p['positionAmt']) > 0:
#                 side = 'SELL'
#             elif float(p['positionAmt']) < 0:
#                 side = 'BUY'
#             else:
#                 continue
#             qty = abs(float(p['positionAmt']))
#             client.futures_create_order(
#                 symbol=symbol,
#                 side=side,
#                 type='MARKET',
#                 quantity=qty
#             )
#             print(f"平仓{symbol}成功！")
# sell_all()


def buy(symbols):
    balances = client.futures_account_balance()  # 获取永续合约账户资产余额
    for balance in balances:
        if balance['asset'] == 'USDT':
            usdt_balance = balance['balance']
    print('USDT余额：', usdt_balance)
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
            # 首先需要找到包含目标交易对数据的那个子字典
            for symbol_info in symbol_dict['symbols']:
                if symbol_info['symbol'] == f'{str(symbol)}':
                    symbol_info = symbol_info
                    break
            print(symbol_info)
            # 精度设置
            price_precision = symbol_info['filters'][0]['minPrice']
            price_precision = abs(int(math.log10(float(price_precision))))
            print(f"{symbol}价格精度: {price_precision}")
            precision = symbol_info['filters'][1]['minQty']
            precision = abs(int(math.log10(float(precision))))
            print(f"{symbol}数量精度: {precision}")
            tickSize = float(symbol_info['filters'][0]['tickSize'])
            print(f"{symbol}价格步长: {tickSize}")
            stepSize = float(symbol_info['filters'][1]['minQty'])
            print(f"{symbol}数量步长: {stepSize}")
            # 实时获取当前卖一和卖二价格
            depth = client.futures_order_book(symbol=symbol, limit=5)
            buy_ask_price_1 = float(depth['asks'][0][0])
            buy_bid_price_1 = float(depth['bids'][0][0])
            print(depth)
            # 计算买卖均价
            target_price = (buy_ask_price_1+buy_bid_price_1)/2
            buy_limit_price = round(
                buy_ask_price_1 - pow(0.1, price_precision), price_precision)
            sell_limit_price = round(
                buy_bid_price_1 + pow(0.1, price_precision), price_precision)
            print('最优卖价', sell_limit_price, '最优买价', buy_limit_price)

            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-buy_bid_price_1/target_price >= 0.001 or buy_ask_price_1/target_price-1 <= 0.001:
                # 下单
                symbol = symbol
                quantity = round(
                    round(12/buy_limit_price/stepSize) * stepSize, precision)
                order = client.futures_create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=float(quantity),
                    price=float(buy_limit_price),
                    timeInForce="GTC",  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                    leverage=1,  # 这里设置杠杆倍数为 1 倍
                    isIsolated=False  # 这里设置使用全仓模式
                )
                print("下单信息：", order)
                collection_write.insert_one({
                    'orderId': int(order['orderId']),
                    'symbol': symbol,
                    'time': int(time.time()),
                    'quantity': float(order['origQty']),
                    'buy_quantity': None,
                    'buy_price': float(order['price']),
                    'sell_time': None,
                    'sell_quantity': None,
                    'sell_price': None,
                    'status': 'pending',
                    'precision': int(precision),
                    'price_precision': int(price_precision),
                    'tickSize': float(tickSize),
                    'stepSize': float(stepSize),
                })
        except Exception as e:
            # 错误次数加1，并输出错误信息
            error_count += 1
            print(f"发生bug: {e}")
            continue
    # 输出异常次数
    print(f"共出现{error_count}次异常")
    # 暂停1秒，等待成交
    time.sleep(1)
    # 撤销未成交订单
    for order in collection_write.find({'status': 'pending'}):
        symbol = order['symbol']
        order_id = order['orderId']
        try:
            # 撤销订单
            result = client.futures_cancel_order(
                symbol=symbol, orderId=order_id)
            print(f'撤销订单{order_id}成功:{result}')
        except Exception as e:
            print(f'撤销订单{order_id}失败{e}')
    # 暂停1秒，等待撤单
    time.sleep(1)


def sell(symbols):
    # 首先更新订单状态
    try:
        for symbol in symbols:
            # 获取当前未完成的订单
            open_orders = client.futures_get_open_orders(symbol=symbol)
            # 遍历未完成的订单
            for order in open_orders:
                order_id = order['orderId']
                buy_price = float(order['price'])
                buy_quantity = float(order['executedQty'])
                # 已成交订单
                collection_write.update_one(
                    {'orderId': order_id},
                    {'$set': {
                        'status': order['status'],
                        'buy_price': float(buy_price),
                        'buy_quantity': float(buy_quantity)
                    }}
                )
                print('历史成交订单更新', order)
            # 获取当前已完成的订单（1小时内）
            start_time = int((datetime.datetime.now() -
                              datetime.timedelta(hours=1)).timestamp() * 1000)
            all_orders = client.futures_get_all_orders(
                symbol=symbol, startTime=start_time)
            # 遍历已完成的订单
            for order in all_orders:
                order_id = order['orderId']
                buy_price = float(order['price'])
                buy_quantity = float(order['executedQty'])
                # 已成交订单
                collection_write.update_one(
                    {'orderId': order_id},
                    {'$set': {
                        'status': order['status'],
                        'buy_price': float(buy_price),
                        'buy_quantity': float(buy_quantity)
                    }}
                )
                print('历史成交订单更新', order)
    except Exception as e:
        print(f"发生异常：{e}")
    try:
        # 查询已下单且未卖出的订单
        orders = list(collection_write.find())
        for order in orders:
            symbol = order['symbol']
            # 首先需要找到包含目标交易对数据的那个子字典
            for symbol_info in symbol_dict['symbols']:
                if symbol_info['symbol'] == f'{str(symbol)}':
                    symbol_info = symbol_info
                    break
            print(symbol_info)
            # 精度设置
            price_precision = symbol_info['filters'][0]['minPrice']
            price_precision = abs(int(math.log10(float(price_precision))))
            print(f"{symbol}价格精度: {price_precision}")
            precision = symbol_info['filters'][1]['minQty']
            precision = abs(int(math.log10(float(precision))))
            print(f"{symbol}数量精度: {precision}")
            tickSize = float(symbol_info['filters'][0]['tickSize'])
            print(f"{symbol}价格步长: {tickSize}")
            stepSize = float(symbol_info['filters'][1]['minQty'])
            print(f"{symbol}数量步长: {stepSize}")
            # 实时获取当前卖一和卖二价格
            depth = client.futures_order_book(symbol=symbol, limit=5)
            sell_ask_price_1 = float(depth['asks'][0][0])
            sell_bid_price_1 = float(depth['bids'][0][0])
            print(depth)
            # 计算买卖均价
            target_price = (sell_ask_price_1+sell_bid_price_1)/2
            buy_limit_price = round(
                sell_ask_price_1 - pow(0.1, price_precision), price_precision)
            sell_limit_price = round(
                sell_bid_price_1 + pow(0.1, price_precision), price_precision)
            print('最优卖价', sell_limit_price, '最优买价', buy_limit_price)

            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-sell_bid_price_1/target_price >= 0.001 or sell_ask_price_1/target_price-1 <= 0.001:
                # 计算卖出时间
                # sell_time = order['time'] + 86400
                sell_time = order['time']

            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-sell_bid_price_1/target_price >= 0.001 or sell_ask_price_1/target_price-1 <= 0.001:
                # 如果订单尚未完全成交，则尝试卖出
                if (order['status'] != 'end') & (order['buy_quantity'] != 0) & (order['buy_quantity'] != order['sell_quantity']):
                    # 计算卖出时间
                    # sell_time = order['time'] + 86400
                    sell_time = order['time']
                    # 如果卖出时间已经到了，就执行卖出操作
                if int(time.time()) >= sell_time:
                    # 卖出订单
                    sell_order = client.futures_create_order(
                        symbol=order['symbol'],
                        side=Client.SIDE_SELL,  # 平多仓设置为卖出，平空仓设置为买入
                        type=Client.ORDER_TYPE_LIMIT,
                        quantity=float(order['buy_quantity']),
                        price=sell_bid_price_1,
                        timeInForce="GTC",  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                        leverage=1,  # 这里设置杠杆倍数为 1 倍
                        isIsolated=False  # 这里设置使用全仓模式
                    )
                    print("卖出信息：", sell_order)
                    # 如果卖出成功，就更新数据集合的状态为已平仓
                    if sell_order['status'] == 'FILLED':
                        print(order, '卖出成功')
                        collection_write.update_one(
                            {'orderId': order_id},
                            {'$set': {
                                'status': 'end',
                                'sell_quantity': float(sell_order['price']),
                                'sell_price': float(sell_order['price']),
                            }}
                        )
                    else:
                        print(order, '卖出失败')
    except Exception as e:
        print(f"发生异常：{e}")


def clearn():
    limit = 10000
    if collection_write.count_documents({}) >= limit:
        oldest_data = collection_write.find().sort([('日期', 1)]).limit(
            collection_write.count_documents({})-limit)
        ids_to_delete = [data['_id'] for data in oldest_data]
        collection_write.delete_many({'_id': {'$in': ids_to_delete}})
    print('数据清理成功')


while True:
    buy(symbols)
    time.sleep(60)
    sell(symbols)
    time.sleep(60)
    clearn()
    time.sleep(3600)