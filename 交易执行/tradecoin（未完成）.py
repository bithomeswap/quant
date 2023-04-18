#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import schedule
from binance.client import Client
from pymongo import MongoClient

# 需要读取的数据库配置
client_read = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db_read = client_read["wth000"]
collection_read = db_read['BTC']

# 需要写入的数据库配置
client_write = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db_write = client_write["wth000"]
collection_write_order = db_write['BTCorder']
collection_write_sell = db_write['BTCsell']

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
client = Client(api_key, api_secret)


def buy():
    # 首先需要获取所有计划交易的标的，包含symbol、时间、价格、日交易量等等信息
    symbols = db_read['COINsymbols']

    for symbol in symbols:
        try:
            # 实时获取当前卖一和卖二价格
            depth = client.get_order_book(symbol=symbol, limit=5)
            ask_price_1 = float(depth['asks'][0][0])
            ask_price_2 = float(depth['asks'][1][0])

            # 计算预定价格
            target_price = ask_price_2 * 1.001

            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if ask_price_1 <= target_price and ask_price_2/ask_price_1 <= 1.01:
                # 下单
                symbol = str(symbol)
                quantity = 1
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
        except:
            print(f"发生bug")
            continue



def sell():
    # 获取当前时间戳
    current_time = int(time.time())

    # 查询已下单且未卖出的订单
    orders = collection_write_order.find({
        'status': 'pending',
        'sell_time': None
    })

    for order in orders:
        # 计算卖出时间
        sell_time = order['time'] + 86400

        # 如果卖出时间已经到了，就执行卖出操作
        if current_time >= sell_time:
            # 卖出订单
            sell_order = client.order_market_sell(
                symbol=order['symbol'],
                quantity=order['quantity']
            )
            print("卖出信息：", sell_order)

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
            collection_write_order.delete_one({'orderId': order['orderId']})


# 每5分钟执行一次买入操作
schedule.every(60).minutes.do(buy)

# 每分钟执行一次卖出操作
schedule.every(60).minutes.do(sell)

while True:
    schedule.run_pending()
    time.sleep(1)
