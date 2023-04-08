#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import schedule
from binance.client import Client
from pymongo import MongoClient

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要读取的数据库配置
client_read = MongoClient(
    'mongodb://username:password@127.0.0.1:27017/dbname?authSource=authdb')
db_read = client_read['dbname']
collection_read = db_read['BTC']

# 需要写入的数据库配置
client_write = MongoClient(
    'mongodb://username:password@127.0.0.1:27017/dbname?authSource=authdb')
db_write = client_write['dbname']
collection_write = db_write['BTCorder']

# 创建Binance客户端
client = Client(api_key, api_secret)


def sell():
    # 获取七天前的时间戳
    time_7days_ago = int(time.time()) - 604800
    # 查询七天前未卖出的买入订单
    orders = collection_write.find({
        "type": "buy",
        "sell_time": {"$eq": None},
        "create_time": {"$lte": time_7days_ago}
    })

    for order in orders:
        # 卖出订单
        sell_order = client.create_order(
            symbol='BTCUSDT',
            side='SELL',
            type='MARKET',
            quantity=order["quantity"]
        )

        # 更新订单信息
        collection_write.update_one(
            {"_id": order["_id"]},
            {"$set": {
                "sell_time": int(time.time()),
                "sell_price": sell_order['price']
            }}
        )


def buy():
    # 获取最新的KDJ_J值
    kdj_j_value = collection_read.find().sort(
        [("time", -1)]).limit(1)[0]["kdj_j"]
    if kdj_j_value < 8:
        # 买入订单
        buy_order = client.create_order(
            symbol='BTCUSDT',
            side='BUY',
            type='MARKET',
            quantity=0.01
        )

        # 插入订单信息
        collection_write.insert_one({
            "type": "buy",
            "create_time": int(time.time()),
            "quantity": buy_order["executedQty"],
            "buy_price": buy_order["price"],
            "sell_time": None,
            "sell_price": None
        })


# 每天晚上9点执行卖出操作
schedule.every().day.at('21:00').do(sell)

# 每5分钟执行一次买入操作
schedule.every(5).minutes.do(buy)

while True:
    schedule.run_pending()
    time.sleep(1)

# 定时到第二三天直接卖出