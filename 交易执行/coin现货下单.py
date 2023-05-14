import math
import datetime
import time
import schedule
from binance.client import Client
from pymongo import MongoClient
# 发生bug: APIError(code=-4131): The counterparty's best price does not meet the PERCENT_PRICE filter limit.
# 针对波场的-4131是市场交易量过小时下单可能导致市场偏离的市场保护机制
# 卖出和买入的价格是0，说明订单尚未成交

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

# 币安的api配置(现货测试网)
api_key = "ERqD8OaGhyPEhZciJs8XZgy7d83x1cyF7U3s2TYw0wZR2SCZMNmf8MzZ8TYzuIzT"
api_secret = "CF2XpFnKKrasCiyZ38D3duMLqFseanJ6iRbmXmrDvqNFlIWJXZbhrlvha5kGxFjr"
# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True)

# 币安的api配置(期货测试网)
# api_key = "266950dec031270d32fed06a552c2698cf662f0e32c4788acf25646bba7ef2c6"
# api_secret = "1a2b9793419db20e99a307be8ac04fec7a43bd77d46b71b161456105161b164d"
# # 创建Binance客户端
# client = Client(api_key, api_secret, testnet=True,
#                 base_endpoint='https://testnet.binancefuture.com/fapi')

# 连接MongoDB数据库
dbclient = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = dbclient["wth000"]
name = "COIN"
collection_write = db[f'{name}order']
# 获取计划交易的标的
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'TRXUSDT']

# def sell_all():  # 市价卖出所有代币
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


def buy(symbols):
    balances = client.get_account()['balances']  # 获取现货账户资产余额
    for balance in balances:
        if balance['asset'] == 'USDT':
            usdt_balance = float(balance['free'])
            print('USDT余额：', usdt_balance)
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
            symbol_info = client.get_symbol_info(symbol)
            print(symbol_info)
            # 针对现货市场的精度设置
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
            target_price = round((ask_price_1+bid_price_1)/2, price_precision)
            buy_limit_price = round(
                ask_price_1 - pow(0.1, price_precision), price_precision)
            sell_limit_price = round(
                bid_price_1 + pow(0.1, price_precision), price_precision)
            print('最优卖价', sell_limit_price, '最优买价', buy_limit_price)
            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-bid_price_1/target_price >= 0.001 or ask_price_1/target_price-1 <= 0.001:
                # 下单
                symbol = symbol
                quantity = round(
                    round(12/buy_limit_price/stepSize) * stepSize, precision)
                # order = client.order_market_buy(symbol=symbol,quantity=quantity)# 市价成交
                order = client.create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=float(quantity),
                    price=float(buy_limit_price),
                    timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
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
            result = client.cancel_order(
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
            symbol_info = client.get_symbol_info(symbol)
            print(symbol_info)
            # 获取当前未完成的订单
            open_orders = client.get_open_orders(symbol=symbol)
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
            all_orders = client.get_all_orders(
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
    print('订单卖出')
    try:
        # 查询已下单且未卖出的订单
        orders = list(collection_write.find())
        for order in orders:
            symbol = order['symbol']
            symbol_info = client.get_symbol_info(symbol)
            print(symbol_info)
            # 针对现货市场的精度设置
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
            target_price = round((ask_price_1+bid_price_1)/2, price_precision)
            buy_limit_price = round(
                ask_price_1 - pow(0.1, price_precision), price_precision)
            sell_limit_price = round(
                bid_price_1 + pow(0.1, price_precision), price_precision)
            print('最优卖价', sell_limit_price, '最优买价', buy_limit_price)
            # 判断当前卖一不高于预定价格，卖二卖一差距较小
            if 1-bid_price_1/target_price >= 0.001 or ask_price_1/target_price-1 <= 0.001:
                # 如果订单尚未完全成交，则尝试卖出
                if (order['status'] != 'end') & (order['buy_quantity'] != 0) & (order['buy_quantity'] != order['sell_quantity']):
                    # 计算卖出时间
                    sell_time = order['time'] + 86400
                    # sell_time = order['time']
                    # 如果卖出时间已经到了，就执行卖出操作
                    if int(time.time()) >= sell_time:
                        # 卖出订单
                        sell_order = client.create_order(
                            symbol=symbol,
                            side=Client.SIDE_SELL,
                            type=Client.ORDER_TYPE_LIMIT,
                            quantity=float(order['buy_quantity']),
                            price=bid_price_1,
                            timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                        )
                        print("卖出信息：", sell_order)
                    # 如果卖出成功，就更新数据集合的状态为已平仓
                    if sell_order['status'] == 'FILLED':
                        print(order, '卖出成功')
                        collection_write.update_one(
                            {'orderId': order_id},
                            {'$set': {
                                'status': 'end',
                                'sell_quantity': float(order['buy_quantity']),
                                'sell_price': bid_price_1,
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
