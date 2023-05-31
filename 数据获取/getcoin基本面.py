import requests
import datetime
from pymongo import MongoClient
# 需要写入的数据库配置
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
# 设置参数
name = "COIN"
collection = db[f"{name}基本面"]
response = requests.get('https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest',
                        headers={
                            'Accepts': 'application/json',
                            'X-CMC_PRO_API_KEY': '5d4d4a5e-a08b-4a90-9aa4-af5dca02f5a4'
                        },
                        params={
                            'start': '1',
                            'limit': '5000',
                            'convert': 'USD'
                        })
data_list = []
if response.status_code == 200:
    data = response.json()
    for coin in data['data']:
        circulating_supply = float(coin['circulating_supply'])
        latest_data = collection.find_one({"代码": str(coin['symbol'])+"USDT"})
        existing_data = collection.find_one_and_update(
            {"代码": str(coin['symbol'])+"USDT"},
            {"$set": {
                "日期":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "代码": str(coin['symbol'])+"USDT",
                "市值": float(coin['quote']['USD']['market_cap']),
                "流通量": float(coin['circulating_supply']),
                "发行量": float(coin['total_supply']),
            }},
            upsert=True,
            return_document=True
        )
else:
    print("请求出错")