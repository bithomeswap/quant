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
collectionbuy = db[f"order买入{name}"]
collectionsell = db[f"order卖出{name}"]
collectionbalance = db[f"order余额{name}"]
# 获取计划交易的标的
buy_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "TRXUSDT"]
sell_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "TRXUSDT"]
money = 10000  # 设置每一批的下单金额
holdday = 1  # 设置持仓周期
waittime = 5  # 设置下单间隔，避免权重过高程序暂停,目前来看5比较好
buy_limit_money = 12  # 设置买单的最小下单金额，不得低于12否则无法成交
sell_limit_money = 12  # 设置卖单的最小下单金额，不得低于12否则无法成交
cancelrate = 5  # 设置撤单速率，每下多少轮次的订单，撤销一次订单


def sell_all():  # 市价卖出所有代币
    # 获取账户余额
    balances = client.get_account()["balances"]
    for balance in balances:
        try:
            asset = balance["asset"]
            free_balance = float(balance["free"])
            locked_balance = float(balance["locked"])
            total_balance = free_balance + locked_balance
            if asset != "USDT" and total_balance > 0:
                symbol = asset + "USDT"
                # 执行市价卖单
                client.order_market_sell(
                    symbol=symbol,
                    quantity=total_balance
                )
                print(f"卖出{asset}成功！")
        except Exception as e:
            print(f"buy发生bug: {e}")
            continue


async def buy(buy_symbol, money):
    balances = client.get_account()["balances"]  # 获取现货账户资产余额
    for balance in balances:
        if balance["asset"] == "USDT":
            usdt_balance = float(balance["free"])
            print("USDT余额：", usdt_balance)
    buymoney = 0  # 累计买入金额
    buyvalue = 0  # 累计买入量
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
            while True:
                # 实时获取当前卖一和买一价格
                buy_depth = client.get_order_book(symbol=buy_symbol, limit=5)
                print(buy_depth)
                buy_ask_price_1 = float(buy_depth["asks"][0][0])
                buy_ask_value_1 = float(buy_depth["asks"][0][1])
                buy_bid_price_1 = float(buy_depth["bids"][0][0])
                buy_bid_value_1 = float(buy_depth["bids"][0][1])
                buy_ave = client.get_avg_price(symbol=buy_symbol)[
                    "price"]  # 获取交易标的的一分钟均价
                # 计算最佳买单和最佳卖单
                buy_target_price = round(
                    (buy_ask_price_1+buy_bid_price_1)/2, buy_price_precision)
                buy_quantity = round(round(
                    buy_limit_money/buy_target_price / buy_stepSize) * buy_stepSize, buy_precision)
                if ((float(buy_target_price)/float(buy_ave)) <= 1.0005) & ((float(buy_target_price)/float(buy_ave)) >= 0.9995):
                    # 上涨的时候会抬升目标价，如果目标价抬升的偏离均价千分之一就暂停下单
                    break
            buy_order = client.create_order(
                symbol=buy_symbol,
                side=Client.SIDE_BUY,
                type=Client.ORDER_TYPE_LIMIT,
                quantity=float(buy_quantity),
                price=float(buy_target_price),
                timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
            )  # 限价成交
            print(f"buy第{n}次下单", "交易标的", buy_symbol, "目标价格", buy_target_price,
                  "买一价:", buy_bid_price_1, "买一量:", buy_bid_value_1,
                  "卖一价:", buy_ask_price_1, "卖一量:", buy_ask_value_1,
                  "数量精度:", buy_precision, "数量步长:", buy_stepSize,
                  "价格精度:", buy_price_precision, "价格步长:", buy_tickSize,
                  "下单信息:", buy_order)
            collectionbuy.insert_one({
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
                "status": "pending",
                "目标买入金额": money,
                "当前下单批次": n,
            })
            if n % cancelrate == 0:
                try:
                    for buy_cancel_order in collectionbuy.find({"status": "pending"}):
                        buy_cancel_symbol = buy_cancel_order["symbol"]
                        buy_cancel_order_id = buy_cancel_order["orderId"]
                        # 撤销订单
                        buy_result = client.cancel_order(
                            symbol=buy_cancel_symbol, orderId=buy_cancel_order_id)
                        print(f"撤销订单{buy_cancel_order_id}成功buy:{buy_result}")
                        # 暂停5秒，等待撤单
                        await asyncio.sleep(waittime)
                except Exception as e:
                    print(f"撤销订单{buy_cancel_order_id}失败buy{e}")
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
                        collectionbuy.update_one(
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
            if buymoney >= money*0.995:
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
    print(f"buy共出现{error_count}次异常")


async def sell(sell_symbol):
    try:
        balancevalue = list(collectionbalance.find({"symbol": sell_symbol, "日期": (datetime.datetime.now(
        ) - datetime.timedelta(days=holdday)).strftime("%Y-%m-%d"), "symbol": sell_symbol}))
        balancevalue = [b["买入数量"] for b in balancevalue][-1]
        # 列表索引不能是字符串
        print("需卖出数量", sell_symbol, balancevalue)
        sellmoney = 0  # 累计卖出金额
        sellvalue = 0  # 累计卖出量
        error_count = 0
        for n in range(1, 86400):
            try:
                # 暂停5秒，等待撤单
                await asyncio.sleep(waittime)
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
                while True:
                    # 实时获取当前卖一和买一价格
                    sell_depth = client.get_order_book(symbol=sell_symbol, limit=5)
                    print(sell_depth)
                    sell_ask_price_1 = float(sell_depth["asks"][0][0])
                    sell_ask_value_1 = float(sell_depth["asks"][0][1])
                    sell_bid_price_1 = float(sell_depth["bids"][0][0])
                    sell_bid_value_1 = float(sell_depth["bids"][0][1])
                    sell_ave = client.get_avg_price(symbol=sell_symbol)[
                        "price"]  # 获取交易标的的一分钟均价
                    # 计算最佳买单和最佳卖单
                    sell_target_price = round(
                        (sell_ask_price_1+sell_bid_price_1)/2, sell_price_precision)
                    sell_quantity = round(round(
                        sell_limit_money/sell_target_price / sell_stepSize) * sell_stepSize, sell_precision)
                    if ((float(sell_target_price)/float(sell_ave)) <= 1.0005) & ((float(sell_target_price)/float(sell_ave)) >= 0.9995):
                        # 卖出的时候会砸低目标价，如果目标价砸低的偏离均价千分之一就暂停下单
                        break
                sell_order = client.create_order(
                    symbol=sell_symbol,
                    side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=float(sell_quantity),
                    price=float(sell_target_price),
                    timeInForce="GTC"  # “GTC”（成交为止），“IOC”（立即成交并取消剩余）和“FOK”（全部或无）
                )  # 限价成交
                print(f"sell第{n}轮下单", "交易标的", sell_symbol, "目标价格", sell_target_price,
                      "买一价:", sell_bid_price_1, "买一量:", sell_bid_value_1,
                      "卖一价:", sell_ask_price_1, "卖一量:", sell_ask_value_1,
                      "数量精度:", sell_precision, "数量步长:", sell_stepSize,
                      "价格精度:", sell_price_precision, "价格步长:", sell_tickSize,
                      "下单信息:", sell_order)
                collectionsell.insert_one({
                    "日期": datetime.datetime.now().strftime("%Y-%m-%d"),
                    "orderId": int(sell_order["orderId"]),
                    "symbol": sell_symbol,
                    "time": int(time.time()),
                    "quantity": float(sell_order["origQty"]),
                    "sell_quantity": None,
                    "sell_price": float(sell_order["price"]),
                    "sell_precision": int(sell_precision),
                    "sell_price_precision": int(sell_price_precision),
                    "sell_tickSize": float(sell_tickSize),
                    "sell_stepSize": float(sell_stepSize),
                    "status": "pending",
                    "目标卖出数量": balancevalue,
                    "当前下单批次": n,
                })
                if n % cancelrate == 0:
                    try:
                        for sell_cancel_order in collectionsell.find({"status": "pending"}):
                            sell_cancel_symbol = sell_cancel_order["symbol"]
                            sell_cancel_order_id = sell_cancel_order["orderId"]
                            # 撤销订单
                            sell_result = client.cancel_order(
                                symbol=sell_cancel_symbol, orderId=sell_cancel_order_id)
                            print(
                                f"撤销订单{sell_cancel_order_id}成功sell:{sell_result}")
                            # 暂停5秒，等待撤单
                            await asyncio.sleep(waittime)
                    except Exception as e:
                        print(f"撤销订单{sell_cancel_order_id}失败sell{e}")
                    # 每10秒更新一次订单状态
                    try:
                        # 获取当日已完成的订单
                        start_time = int(
                            (datetime.datetime.now() - datetime.timedelta(days=1)).timestamp() * 1000)
                        all_sell_orders = client.get_all_orders(
                            symbol=sell_symbol, startTime=start_time)
                        # 遍历已完成的订单
                        for all_sell_order in all_sell_orders:
                            all_sell_order_id = all_sell_order["orderId"]
                            # 已成交订单
                            collectionsell.update_one(
                                {"orderId": all_sell_order_id},
                                {"$set": {
                                    "sell_price": float(all_sell_order["price"]),
                                    "sell_quantity": float(all_sell_order["executedQty"]),
                                    "status": all_sell_order["status"],
                                }}
                            )
                            sellmoney += float(all_sell_order["price"]) * float(
                                all_sell_order["executedQty"])
                            sellvalue += float(all_sell_order["executedQty"])
                        print(f"待下单金额：{sellmoney}", f"今日买入量：{sellvalue}",
                              "历史成交订单更新sell", all_sell_order)
                    except Exception as e:
                        print(f"发生异常：{e}")
                if sellvalue >= balancevalue*0.995:
                    collectionbalance.update_one(
                        {"symbol": sell_symbol, "日期": (datetime.datetime.now(
                        ) - datetime.timedelta(days=holdday)).strftime("%Y-%m-%d")},
                        {"$set": {
                            "卖出金额": sellmoney,
                            "卖出数量": sellvalue,
                        }}
                    )
                    break
            except Exception as e:
                # 错误次数加1，并输出错误信息
                error_count += 1
                print(f"sell发生bug: {e}")
                continue
        # 输出异常次数
        print(f"sell共出现{error_count}次异常")
    except Exception as e:
        print(e)


async def clearn():
    limit = 10000
    for collection in [collectionbuy, collectionsell, collectionbalance]:
        if collection.count_documents({}) >= limit:
            oldest_data = collection.find().sort([("日期", 1)]).limit(
                collection.count_documents({})-limit)
            ids_to_delete = [data["_id"] for data in oldest_data]
            collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(f"{collection}数据清理成功")


async def main():
    tasks = []
    for buy_symbol in buy_symbols:
        tasks.append(asyncio.create_task(buy(buy_symbol, money)))
    for sell_symbol in sell_symbols:
        tasks.append(asyncio.create_task(sell(sell_symbol)))
    tasks.append(asyncio.create_task(clearn()))
    await asyncio.gather(*tasks)
if __name__ == "__main__":
    # sell_all()
    asyncio.run(main())
