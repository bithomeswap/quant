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

symbol="BUSDUSDT"

config_logging(logging, logging.DEBUG)


def on_message(_, data):
    json_msg = json.loads(data)
    logging.info(json_msg)
    # if json_msg.__contains__('lastUpdateId') and json_msg.__contains__('asks') and json_msg.__contains__('bids'):
    #     data = json_msg['data']
    #     timestamp = int(data['E'])
    #     if collection.find({'timestamp': timestamp, 'data': data}).count() == 0:
    #         collection.insert_one({'timestamp': timestamp, 'data': data})
    #         logging.info(
    #             '已成功插入:' + str({'timestamp': timestamp, 'data': data}))

        

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
    