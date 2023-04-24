# -*- coding: utf-8 -*-
# 指定解释器位置
#!/root/miniconda3/bin/python
# nohup /root/miniconda3/bin/python /root/binance_trade_tool_vpn/quant/数据获取/getbtc指标.py
# 安装币安的python库
# pip install python-binance
from binance.client import Client
from pymongo import MongoClient
import time
import talib
# 币安的api配置
api_key = "0jmNVvNZusoXKGkwnGLBghPh8Kmc0klh096VxNS9kn8P0nkAEslVUlsuOcRoGrtm"
api_secret = "PbSWkno1meUckhmkLyz8jQ2RRG7KgmZyAWhIF0qPdCJrmDSFxoxGdMG5gZeYYCgy"

# 需要写入的数据库配置
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'COIN实盘'
collection = db[f'{name}']
collection.drop()  # 清空集合中的所有文档

# 创建Binance客户端
client = Client(api_key, api_secret)

# 获取所有USDT计价的现货交易对
ticker_prices = client.get_exchange_info()['symbols']

usdt_ticker_prices = [
    ticker_price for ticker_price in ticker_prices if ticker_price['quoteAsset'] == 'USDT' and ("DOWN" not in ticker_price['symbol']) and ("UP" not in ticker_price['symbol'])]

print(f"当前币安现货有{len(ticker_prices)}个交易对")
# 遍历所有现货交易对，并获取日K线数据
data_list = []
for ticker_price in usdt_ticker_prices:
    symbol = ticker_price['symbol']
    klines = client.get_historical_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1DAY,
        limit=150
    )
    # 插入到集合中
    for kline in klines:
        timestamp = kline[0] / 1000
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
        # 如果没有，则加入到数据列表中
        data_list.append({'timestamp': timestamp,
                          '代码': symbol,
                          '日期': date,
                          '开盘': float(kline[1]),
                          '最高': float(kline[2]),
                          '最低': float(kline[3]),
                          '收盘': float(kline[4]),
                          '成交量': float(kline[5]),
                          '收盘timestamp': float(kline[6]/1000),
                          '成交额': float(kline[7]),
                          '成交笔数': float(kline[8]),
                          '主动买入成交量': float(kline[9]),
                          '主动买入成交额':  float(kline[10])
                          })


def get_technical_indicators(df):  # 定义计算技术指标的函数
    # 过滤最高价和最低价为负值的数据
    df = df.loc[(df['最高'] >= 0) & (df['最低'] >= 0)]
    df = df.sort_values(by='日期')    # 以日期列为索引,避免计算错误

    # 计算昨日成交额
    df['昨日成交额'] = df.shift(1)['成交额'].astype(float)
    # 定义开盘收盘幅
    df['开盘收盘幅'] = (df['开盘']/df.shift(1)['收盘'] - 1)*100
    # 定义收盘幅即涨跌幅
    df['涨跌幅'] = (df['收盘']/df.shift(1)['收盘'] - 1)*100
    df = df.dropna()  # 删除缺失值，避免无效数据的干扰
    for n in range(1, 14):  # 计算未来n日涨跌幅
        df[f'{n*10}日最低开盘价比值'] = df['开盘']/df['开盘'].rolling(n*10).min()
        df[f'SMA{n*10}开盘比值'] = df['开盘'] / \
            talib.MA(df['开盘'].values, timeperiod=n*10, matype=0)
    return df

# 按照“代码”列进行分组并计算技术指标
grouped = data_list.groupby('代码').apply(get_technical_indicators)
print('准备插入数据')
# 连接MongoDB数据库并创建新集合
new_collection = db[f'{name}指标']
new_collection.drop()  # 清空集合中的所有文档
# 将数据分批插入
batch_size = 5000  # 批量插入的大小
num_batches = len(grouped) // batch_size + 1
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = (i + 1) * batch_size
    data_slice = grouped[start_idx:end_idx]
    new_collection.insert_many(data_slice.to_dict('records'))
