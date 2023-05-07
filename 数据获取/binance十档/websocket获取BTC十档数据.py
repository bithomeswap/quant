import logging
import json
from decimal import *
from binance.spot import Spot as SpotAPIClient
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_api import SpotWebsocketAPIClient
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import pymongo

# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# mongodb的配置
client = pymongo.MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
collection = db['十档BTC']
global symbol
symbol = "BTCUSDT"

config_logging(logging, logging.DEBUG)


def on_message(_, message):
    data = json.loads(message)  # 将收到的消息解析为JSON对象
    # logging.info(data)
    try:
        global symbol
        symbol = symbol
        bids = [[float(item[0]), float(item[1])] for item in data['bids']]
        asks = [[float(item[0]), float(item[1])] for item in data['asks']]
        lastUpdateId = int(data['lastUpdateId'])  # 获取数据生成时的时间戳

        order_data = {'lastUpdateId': lastUpdateId, 'symbol': symbol}
        for i in range(len(bids)):
            order_data[f"bid_{i+1}_price"] = bids[i][0]
            order_data[f"bid_{i+1}_quantity"] = bids[i][1]
        for i in range(len(asks)):
            order_data[f"ask_{i+1}_price"] = asks[i][0]
            order_data[f"ask_{i+1}_quantity"] = asks[i][1]

        if collection.count_documents({'lastUpdateId': lastUpdateId}) == 0:
            collection.insert_one(order_data)

        # logging.info(f"{symbol} {order_data} {lastUpdateId}")  # 打印数据不要总是输出日志，避免影响负载状态
    except Exception as e:
        print("Error:", e)


def on_close(_):
    logging.info("Do custom stuff when connection is closed")


if __name__ == "__main__":
    # make a connection to the websocket api
    ws_api_client = SpotWebsocketAPIClient(
        stream_url="wss://ws-api.binance.com/ws-api/v3",
        api_key=api_key,
        api_secret=api_secret,
        on_message=on_message,
        on_close=on_close,
    )
    # make a connection to the websocket stream
    ws_stream_client = SpotWebsocketStreamClient(
        stream_url="wss://stream.binance.com:9443",
        on_message=on_message,
    )
    # 实时盘口挂单信息
    ws_stream_client.partial_book_depth(symbol)
