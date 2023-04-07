import time
from binance.websocket.spot.websocket_client import SpotWebsocketClient as Client
from pymongo import MongoClient, ASCENDING

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要写入的数据库配置
client_db = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client_db['wth000']
collection = db["十档BTC"]


def callback(btcusdt):
    if btcusdt['u'] == 0:
        # 初始全量数据
        return
    print('收到来自币安的十档BTC数据：', btcusdt)

    # 将时间戳转换为ISO格式的日期字符串
    timestamp = int(btcusdt['E']) // 1000
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))

    # 插入到数据库中
    if collection.count_documents({'timestamp': timestamp,
                                   '日期': date,
                                   '十档卖盘': btcusdt['a'],
                                   '十档买盘': btcusdt['b']}) == 0:
        collection.insert_one({'timestamp': timestamp,
                               '日期': date,
                               '十档卖盘': btcusdt['a'],
                               '十档买盘': btcusdt['b']})


if __name__ == '__main__':
    # 启动websocket客户端
    client = Client()
    client.start()
    # 订阅BTCUSDT交易对
    client.subscribe_book_ticker(symbol='BTCUSDT', callback=callback)
