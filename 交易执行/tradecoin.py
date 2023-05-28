import asyncio
import math
import datetime
import time
from binance.client import Client
from pymongo import MongoClient
# 函数说明
# get_exchange_info()：获取当前交易对信息，包括交易对精度、限价单最大值、挂单最小值等。
# get_order_book(symbol, limit=100)：获取指定交易对的深度信息，包括卖五、买五等信息。
# get_account(recvWindow=60000)：获取当前账户信息，包括余额、冻结资金等。
# get_avg_price(symbol)：获取指定交易对最新的平均价格。
# get_open_orders(symbol=None, recvWindow=60000)：获取当前账户的所有未成交的订单信息，可以指定交易对。
# cancel_order(symbol, orderId=None, origClientOrderId=None, newClientOrderId=None, recvWindow=60000)：取消指定订单，可以通过订单ID或客户端自定义ID来取消。
# get_order(symbol, orderId=None, origClientOrderId=None, recvWindow=60000)：获取指定订单的信息，可以通过订单ID或客户端自定义ID来查询。
# get_all_orders(symbol, orderId=None, startTime=None, endTime=None, limit=500, recvWindow=60000)：获取当前账户在指定交易对下的所有订单信息。

# # 币安的api配置(主网)
# api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
# api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"
# 创建Binance客户端
# client = Client(api_key, api_secret)

# 币安的api配置(现货测试网)
api_key = "I5To2CwMIp74EB6zkulpwo4eioWPrYyp4JwBDLBR6QFNHalQUnm595ZEy3Z3JWzK"
api_secret = "37DJ4aGGfTTLuNtKHQC7p8IMRN3fx0kM5QY0iZGqFwZ9GDeBfi3YUF3FHCngInH3"
# 创建Binance客户端
client = Client(api_key, api_secret, testnet=True)

# 连接MongoDB数据库
dbclient = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = dbclient["wth000"]
name = "COIN"
collection = db[f"order{name}"]
collectionbalance = db[f"order余额{name}"]
# def sell_all():  # 市价卖出所有代币
#     # 获取账户余额
#     balances = client.get_account()["balances"]
#     for balance in balances:
#         asset = balance["asset"]
#         free_balance = float(balance["free"])
#         locked_balance = float(balance["locked"])
#         total_balance = free_balance + locked_balance
#         if asset != "USDT" and total_balance > 0:
#             symbol = asset + "USDT"
#             # 执行市价卖单
#             client.order_market_sell(
#                 symbol=symbol,
#                 quantity=total_balance
#             )
#             print(f"卖出{asset}成功！")
# sell_all()

# 获取计划交易的标的
buy_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "TRXUSDT"]
sell_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "TRXUSDT"]

money = 600  # 设置每一批的下单金额
holdday = 0  # 设置持仓周期
waittime = 5  # 设置下单间隔，避免权重过高程序暂停,目前来看5比较好


async def buy(buy_symbol, money):
    buymoney = 0  # 累计买入金额
    buyvalue = 0  # 累计买入量
    balances = client.get_account()["balances"]  # 获取现货账户资产余额
    for balance in balances:
        if balance["asset"] == "USDT":
            usdt_balance = float(balance["free"])
            print("USDT余额：", usdt_balance)
    # 添加异常计数器
    error_count = 0
    for n in range(1, 86400):
        try:
            # 暂停5秒，等待撤单
            await asyncio.sleep(waittime)
            buy_symbol_info = client.get_symbol_info(buy_symbol)
            # 针对现货市场的精度设置
            buy_price_precision = buy_symbol_info["filters"][0]["minPrice"]
            buy_price_precision = abs(
                int(math.log10(float(buy_price_precision))))
            buy_precision = buy_symbol_info["filters"][1]["minQty"]
            buy_precision = abs(int(math.log10(float(buy_precision))))
            buy_tickSize = float(buy_symbol_info["filters"][0]["tickSize"])
            buy_stepSize = float(buy_symbol_info["filters"][1]["minQty"])
            # 实时获取当前卖一和买一价格
            buy_depth = client.get_order_book(symbol=buy_symbol, limit=5)
            buy_ask_price_1 = float(buy_depth["asks"][0][0])
            buy_bid_price_1 = float(buy_depth["bids"][0][0])
            # 计算最佳买单和最佳卖单
            buy_bid_limit_price = round(
                buy_ask_price_1 - pow(0.1, buy_price_precision), buy_price_precision)
            buy_ask_limit_price = round(
                buy_bid_price_1 + pow(0.1, buy_price_precision), buy_price_precision)
            quantity = round(round(12/buy_bid_limit_price /buy_stepSize) * buy_stepSize, buy_precision)
            buy_order = client.create_order(
                symbol=buy_symbol,
                side=Client.SIDE_BUY,
                type=Client.ORDER_TYPE_LIMIT,
                quantity=float(quantity),
                price=float(buy_bid_limit_price),
                timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
            )  # 限价成交
            print(f"buy第{n}次下单", "交易标的", buy_symbol,"买一价:",buy_bid_price_1,"最优卖价:", buy_ask_limit_price,"卖一价:",buy_ask_price_1, "最优买价:", buy_bid_limit_price,
                "数量精度:", buy_precision, "数量步长:", buy_stepSize, "价格精度:", buy_price_precision, "价格步长:", buy_tickSize,
                "下单信息:", buy_order)
            collection.insert_one({
                "日期": datetime.datetime.now().strftime("%Y-%m-%d"),
                "orderId": int(buy_order["orderId"]),
                "symbol": buy_symbol,
                "time": int(time.time()),
                "quantity": float(buy_order["origQty"]),
                "buy_quantity": None,
                "buy_price": float(buy_order["price"]),
                "buy_precision": int(buy_precision),
                "buy_price_precision": int(buy_price_precision),
                "buy_tickSize": float(buy_tickSize),
                "buy_stepSize": float(buy_stepSize),
                "sell_time": None,
                "sell_quantity": None,
                "sell_price": None,
                "sell_precision": None,
                "sell_price_precision": None,
                "sell_tickSize": None,
                "sell_stepSize": None,
                "last_orderId": None,
                "status": "pending",
                "目标买入金额": money,
                "当前下单笔次": n,
            })
            if n % 10 == 0:
                # 每10秒撤销一次未成交订单
                try:
                    for cancel_order in collection.find({"status": "pending"}):
                        cancel_symbol = cancel_order["symbol"]
                        cancel_order_id = cancel_order["orderId"]
                        # 撤销订单
                        result = client.cancel_order(
                            symbol=cancel_symbol, orderId=cancel_order_id)
                        print(f"撤销订单{cancel_order_id}成功buy:{result}")
                        # 暂停5秒，等待撤单
                        await asyncio.sleep(waittime)
                except Exception as e:
                    print(f"撤销订单{cancel_order_id}失败buy{e}")
                # 每10秒更新一次订单状态
                try:
                    # 获取当日已完成的订单
                    start_time = int(
                        (datetime.datetime.now() - datetime.timedelta(days=1)).timestamp() * 1000)
                    all_orders = client.get_all_orders(
                        symbol=buy_symbol, startTime=start_time)
                    # 遍历已完成的订单
                    for all_order in all_orders:
                        all_order_id = all_order["orderId"]
                        # 已成交订单
                        collection.update_one(
                            {"orderId": all_order_id},
                            {"$set": {
                                "buy_price": float(all_order["price"]),
                                "buy_quantity": float(all_order["executedQty"]),
                                "status": all_order["status"],
                            }}
                        )
                        buymoney += float(all_order["price"]) * \
                            float(all_order["executedQty"])
                        buyvalue += float(all_order["executedQty"])
                    print(f"待下单金额：{buymoney}", f"今日买入量：{buyvalue}",
                          "历史成交订单更新sell", all_order)
                except Exception as e:
                    print(f"发生异常：{e}")
            if buymoney >= money-150:
                collectionbalance.insert_one(
                    {
                        "日期": datetime.datetime.now().strftime("%Y-%m-%d"),
                        "symbol": buy_symbol,
                        "买入金额": buymoney,
                        "买入数量": buyvalue,
                        "卖出金额": None,
                        "卖出数量": None,
                    }
                )
                break
        except Exception as e:
            # 错误次数加1，并输出错误信息
            error_count += 1
            print(f"buy发生bug: {e}")
            continue
    # 输出异常次数
    print(f"共出现{error_count}次异常")


async def sell(sell_symbol):
    try:
        balancevalue = list(collectionbalance.find({"symbol": sell_symbol, "日期": (datetime.datetime.now(
        ) - datetime.timedelta(days=holdday)).strftime("%Y-%m-%d"), "symbol": sell_symbol}))
        balancevalue = [b["买入数量"] for b in balancevalue][-1]
        # 列表索引不能是字符串
        print("需卖出数量", sell_symbol, balancevalue)
        # 查询已下单且未卖出的订单1
        sell_orders = list(collection.find({"symbol": sell_symbol, "日期": (datetime.datetime.now(
        ) - datetime.timedelta(days=holdday)).strftime("%Y-%m-%d"), "symbol": sell_symbol}))
        sellmoney = 0
        sellvalue = 0
        for n in range(1, 86400):
            for sell_order in sell_orders:
                # 暂停5秒，等待撤单
                await asyncio.sleep(waittime)
                sell_symbol = sell_order["symbol"]
                sell_symbol_info = client.get_symbol_info(sell_symbol)
                # 针对现货市场的精度设置
                sell_price_precision = sell_symbol_info["filters"][0]["minPrice"]
                sell_price_precision = abs(
                    int(math.log10(float(sell_price_precision))))
                sell_precision = sell_symbol_info["filters"][1]["minQty"]
                sell_precision = abs(int(math.log10(float(sell_precision))))
                sell_tickSize = float(
                    sell_symbol_info["filters"][0]["tickSize"])
                sell_stepSize = float(sell_symbol_info["filters"][1]["minQty"])
                # 实时获取当前卖一和买一价格
                sell_depth = client.get_order_book(symbol=sell_symbol, limit=5)
                sell_ask_price_1 = float(sell_depth["asks"][0][0])
                sell_bid_price_1 = float(sell_depth["bids"][0][0])
                # 计算最佳买单和最佳卖单
                sell_bid_limit_price = round(
                    sell_ask_price_1 - pow(0.1, sell_price_precision), sell_price_precision)
                sell_ask_limit_price = round(
                    sell_bid_price_1 + pow(0.1, sell_price_precision), sell_price_precision)
                print(f"sell第{n}轮下单", "交易标的", sell_symbol, "买一价:",sell_bid_price_1,"最优卖价:", sell_ask_limit_price, "卖一价:",sell_ask_price_1,"最优买价:", sell_bid_limit_price,
                      "数量精度:", sell_precision, "数量步长:", sell_stepSize, "价格精度:", sell_price_precision, "价格步长:", sell_tickSize)
                # 判断当前买卖价差不超过千分之二才进行卖出
                if (sell_order["status"] != "END") & (sell_order["buy_quantity"] != 0) & (sell_order["buy_quantity"] != sell_order["sell_quantity"]):
                    # 卖出订单
                    last_order = client.create_order(
                        symbol=sell_symbol,
                        side=Client.SIDE_SELL,
                        type=Client.ORDER_TYPE_LIMIT,
                        quantity=float(sell_order["buy_quantity"]),
                        price=sell_bid_price_1,
                        timeInForce="FOK"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                    )
                # if (sell_order["status"] != "END") & (sell_order["buy_quantity"] != 0) & (sell_order["buy_quantity"] != sell_order["sell_quantity"]):
                #     # 卖出订单
                #     last_order = client.create_order(
                #         symbol=sell_symbol,
                #         side=Client.SIDE_SELL,
                #         type=Client.ORDER_TYPE_LIMIT,
                #         quantity=float(sell_order["buy_quantity"]),
                #         price=sell_bid_price_1,
                #         timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                #     )

                    print(f"sell第{n}轮下单", f"{sell_symbol}卖出成功",
                          "下单信息:", last_order["orderId"])
                    # 如果卖出成功，就更新数据集合的状态为已平仓
                    if last_order["status"] == "FILLED":
                        collection.update_one(
                            {"orderId": last_order["orderId"]},
                            {"$set": {
                                "sell_time": int(time.time()),
                                "sell_quantity": float(last_order["executedQty"]),
                                "sell_price": float(last_order["price"]),
                                "sell_precision": int(sell_precision),
                                "sell_price_precision": int(sell_price_precision),
                                "sell_tickSize": float(sell_tickSize),
                                "sell_stepSize": float(sell_stepSize),
                                "last_orderId": int(last_order["orderId"]),
                                "status": "END",
                            }}
                        )
                        sellvalue += float(last_order["executedQty"])
                        sellmoney += float(last_order["price"]) * \
                            float(last_order["executedQty"])
                        print(sell_order["orderId"],
                              "卖出成功,卖出的orderid为：", last_order)
                    else:
                        print(sell_order, "卖出失败sell")
            if sellvalue >= balancevalue-5:
                break
        print("卖出标的", sell_symbol, "卖出金额", sellmoney, "卖出数量", sellvalue)
        collectionbalance.update_one(
            {"日期": datetime.datetime.now().strftime(
                "%Y-%m-%d"), "symbol": sell_symbol},
            {"$set": {
                "卖出金额": sellmoney,
                "卖出数量": sellvalue,
            }}
        )
    except Exception as e:
        print(f"发生异常：{e}")


async def clearn():
    limit = 10000
    if collection.count_documents({}) >= limit:
        oldest_data = collection.find().sort([("日期", 1)]).limit(
            collection.count_documents({})-limit)
        ids_to_delete = [data["_id"] for data in oldest_data]
        collection.delete_many({"_id": {"$in": ids_to_delete}})
    print("数据清理成功")


async def main():
    tasks = []
    # for buy_symbol in buy_symbols:
    #     tasks.append(asyncio.create_task(buy(buy_symbol, money)))
    for sell_symbol in sell_symbols:
        tasks.append(asyncio.create_task(sell(sell_symbol)))
    tasks.append(asyncio.create_task(clearn()))
    await asyncio.gather(*tasks)
if __name__ == "__main__":
    asyncio.run(main())
