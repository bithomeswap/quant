from binance.client import Client
from pymongo import MongoClient

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
collection = db['minimum_order']

# 创建Binance客户端并获取所有交易对信息
client = Client(api_key, api_secret)
exchange_info = client.get_exchange_info()

# 遍历每个交易对，获取最小下单数量、价格精度、数量精度等信息
for symbol in exchange_info["symbols"]:
    filters = symbol["filters"]
    for filter in filters:
        if filter["filterType"] == "LOT_SIZE":
            min_qty = float(filter["minQty"])
            step_size = float(filter["stepSize"])
            print(
                f"Symbol: {symbol['symbol']}, minQty: {min_qty}, stepSize: {step_size}")
        elif filter["filterType"] == "PRICE_FILTER":
            min_price = float(filter["minPrice"])
            tick_size = float(filter["tickSize"])
            print(
                f"Symbol: {symbol['symbol']}, minPrice: {min_price}, tickSize: {tick_size}")

    # 将交易对和最小下单数量、价格精度、数量精度等信息添加到 MongoDB
    collection.insert_one({
        "symbol": symbol["symbol"],
        "min_qty": min_qty,
        "step_size": step_size,
        "min_price": min_price,
        "tick_size": tick_size,
    })

print("Minimum order amounts have been written to the database.")
