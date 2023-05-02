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

# 获取计划交易的标的
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'TRXUSDT']

symbol_dict = client.futures_exchange_info()


def sell_all():  # 市价平仓所有持仓
    for symbol in symbols:    # 获取当前持仓信息
        position = client.futures_position_information(symbol=symbol)
        # 遍历所有持仓并按相反方向下单平仓
        for p in position:
            if float(p['positionAmt']) > 0:
                side = 'SELL'
            elif float(p['positionAmt']) < 0:
                side = 'BUY'
            else:
                continue
            qty = abs(float(p['positionAmt']))
            client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=qty
            )
            print(f"平仓{symbol}成功！")
sell_all()


balances = client.futures_account_balance()  # 获取永续合约账户资产余额
for balance in balances:
    if balance['asset'] == 'USDT':
        usdt_balance = balance['balance']
print('USDT余额：', usdt_balance)


def buy():
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
            # 首先需要找到包含目标交易对数据的那个子字典
            for symbol_info in symbol_dict['symbols']:
                if symbol_info['symbol'] == f'{str(symbol)}':
                    symbol_info = symbol_info
                    break
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
            depth = client.get_order_book(symbol=symbol, limit=5)
            ask_price_1 = float(depth['asks'][0][0])
            bid_price_1 = float(depth['bids'][0][0])
            print(depth)
            # 计算买卖均价
            target_price = (ask_price_1+bid_price_1)/2
            buy_limit_price = round(
                ask_price_1 - pow(0.1, price_precision), price_precision)
            sell_limit_price = round(
                bid_price_1 + pow(0.1, price_precision), price_precision)
            print('最优卖价', sell_limit_price, '最优买价', buy_limit_price)

            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-bid_price_1/target_price >= 0.001 or ask_price_1/target_price-1 <= 0.001:
                # 下单
                symbol = str(symbol)
                quantity = round(
                    round(12/buy_limit_price/stepSize) * stepSize, precision)
                order = client.futures_create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=quantity,
                    price=buy_limit_price,
                    timeInForce="GTC",  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                    leverage=1,  # 这里设置杠杆倍数为 1 倍
                    isIsolated=False  # 这里设置使用全仓模式
                )
                print("下单信息：", order)
                collection_write_order.insert_one({
                    'orderId': order['orderId'],
                    'time': int(time.time()),
                    'symbol': symbol,
                    'quantity': order['origQty'],
                    'buy_price': order['price'],
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
    # 暂停1秒，等待成交
    time.sleep(1)
    # 撤销未成交订单
    for order in collection_write_order.find({'status': 'pending'}):
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


def check_pending_order():
    try:
        for symbol in symbols:
            # 获取未完成订单
            open_orders = client.futures_get_open_orders(symbol=symbol)
            print('未完成', open_orders)
            # 获取当前已完成的订单（1小时内）
            start_time = int((datetime.datetime.now() -
                             datetime.timedelta(hours=1)).timestamp() * 1000)
            all_orders = client.futures_get_all_orders(
                symbol=symbol, startTime=start_time)
            print('历史成交', all_orders)

    # 函数代码
    except Exception as e:
        print(f"发生异常：{e}")


def sell():
    try:
        # 首先检查未成交的订单
        check_pending_order()
        print('执行卖出计划')
        # 查询已下单且未卖出的订单
        orders = collection_write_order.find({
            'status': 'pending',
            'sell_time': None
        })
        for order in orders:
            # 计算卖出时间
            # sell_time = order['time'] + 86400
            sell_time = order['time']
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

                # if sell_order.get('avgPrice'):
                #     # 更新订单信息
                #     collection_write_order.update_one(
                #         {'orderId': order['orderId']},
                #         {'$set': {
                #             'sell_time': int(time.time()),
                #             'sell_price': float(sell_order['avgPrice']),
                #             'status': 'done'
                #         }}
                #     )

                #     # 插入卖出订单信息
                #     collection_write_sell.insert_one({
                #         'orderId': sell_order['orderId'],
                #         'time': int(time.time()),
                #         'symbol': order['symbol'],
                #         'quantity': order['quantity'],
                #         'buy_price': order['buy_price'],
                #         'sell_price': float(sell_order['avgPrice'])
                #     })
                #     print("已卖出信息：", {
                #         'orderId': sell_order['orderId'],
                #         'time': int(time.time()),
                #         'symbol': order['symbol'],
                #         'quantity': order['quantity'],
                #         'buy_price': order['buy_price'],
                #         'sell_price': float(sell_order['avgPrice'])
                #     })

                #     # 删除已下单信息
                #     collection_write_order.delete_one(
                #         {'orderId': order['orderId']})
                # else:
                #     print("无法卖出订单:", order)
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
