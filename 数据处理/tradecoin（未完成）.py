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

# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)
# 币安的api配置(测试网)
api_key = "VaDDXCX3aXp8tkpsUwUlnqAySkIlVPZPRTDp3yRbyIRDayvl1RZUHWW1z5RusW6k"
api_secret = "qikrabwOmwfZEH74iLV5IJyTl6yXxdAUiqPFO2v3rKbNxEK3PccEEDWcqfem6Dvf"
# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True)

# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = "BTC"
collection = db[f'{name}']
# 获取数据并转换为DataFrame格式
data = pd.DataFrame(list(collection.find()))
print("数据读取成功")
collection_write_order = db[f'{name}order']
collection_write_sell = db[f'{name}sell']

def get_technical_indicators(df):  # 定义计算技术指标的函数
    # 过滤最高价和最低价为负值的数据
    df = df.loc[(df['最高'] >= 0) & (df['最低'] >= 0)]
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
    # 计算昨日振幅
    df['昨日振幅'] = (df.shift(1)['最高']-df.shift(1)['最低'])/df.shift(1)['开盘']
    # 计算昨日成交额
    df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
    # 定义开盘收盘幅
    df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
    df = df.dropna()  # 删除缺失值，避免无效数据的干扰
    for n in range(2, 11):
        df[f'SMA{n*10}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
        df[f'SMA{n*10}昨日成交额比值'] = df['昨日成交额'] / \
            talib.MA(df['昨日成交额'].values, timeperiod=n*10, matype=0)
        df[f'SMA{n}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n, matype=0)
        df[f'SMA{n}昨日成交额比值'] = df['昨日成交额'] / \
            talib.MA(df['昨日成交额'].values, timeperiod=n, matype=0)
    for n in range(1, 11):
        df[f'{n}日后总涨跌幅（未来函数）'] = df['收盘'].shift(-n)/df['收盘']-1
        df[f'{n*10}日后总涨跌幅（未来函数）'] = df['收盘'].shift(-n*10)/df['收盘']-1
    return df


# 按照“代码”列进行分组并计算技术指标
grouped = data.groupby('代码').apply(get_technical_indicators)

# 今日筛选股票推送(多头)
df = grouped.sort_values(by='日期')
# 获取最后一天的数据
last_day = df.iloc[-1]['日期']
# 计算总共统计的股票数量
code_count = len(df['代码'].drop_duplicates())
df = df[df[f'日期'] == last_day].copy()
# 成交额过滤劣质股票
df = df[df[f'昨日成交额'] >= 20000000].copy()
# 60日相对超跌
n_stock = math.ceil(code_count/5)
df = df.nsmallest(n_stock, f'SMA{60}开盘比值')
# 振幅较大，趋势明显
n_stock = math.ceil(code_count/10)
df = df.nsmallest(n_stock, '昨日振幅')
# 确认短期趋势
for n in range(6, 11):
    df = df[df[f'SMA{n}开盘比值'] >= 1].copy()
# 开盘价过滤高滑点股票
df = df[df[f'开盘'] >= 0.01].copy()
print(len(df))


def buy():
    # 首先需要获取所有计划交易的标的，包含symbol、时间、价格、日交易量等等信息
    symbols = df['代码']
    # 添加异常计数器
    error_count = 0
    for symbol in symbols:
        try:
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


# 每5分钟执行一次买入操作
schedule.every(60).minutes.do(buy)

# 每分钟执行一次卖出操作
schedule.every(60).minutes.do(sell)

while True:
    schedule.run_pending()
    time.sleep(1)
