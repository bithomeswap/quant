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
                "日期": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

# 原代码后缀上上述代码,可以进行基本面数据的拼接
# # 数据拼接及指标计算
# time.sleep(1)
# df = pd.DataFrame(list(collection.find()))
# try:
#     dfbase = pd.DataFrame(list(db[f"COIN基本面"].find()))
#     # 仅保留共有代码的数据行
#     common_codes = set(df["代码"]).intersection(set(dfbase["代码"]))
#     df = df[df["代码"].isin(common_codes)]
#     dfbase = dfbase[dfbase["代码"].isin(common_codes)]
#     df = pd.merge(df, dfbase[["代码", "发行量"]], on="代码")
#     df["总市值"] = df["开盘"]*df["发行量"]
#     df.drop('_id', axis=1, inplace=True)  # 删掉目标列
#     db[f"{name}拼接"].drop()
#     time.sleep(1)
#     db[f"{name}拼接"].insert_many(df.to_dict("records"))
#     print("拼接数据插入完成")
# except Exception as e:
#     print(e, "拼接基本面数据失败")