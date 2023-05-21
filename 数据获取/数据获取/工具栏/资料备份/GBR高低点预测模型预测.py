import pytz
import datetime
import pickle
import pandas as pd
from pymongo import MongoClient
import requests
import os
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'BTC'
collection = db[f'{name}指标']
# 读取最新60个文档
df = pd.DataFrame(
    list(collection.find().sort([("timestamp", -1)]).limit(20)))
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
file_path = os.path.join(dir_path, f'{name}model.pickle')
with open(file_path, 'rb') as f:
    model = pickle.load(f)
# 确认特征列
tezheng = [
    'timestamp', '最高', '最低', '开盘', '收盘', '涨跌幅', '开盘收盘幅', '昨日成交额',
]
for n in range(1, 17):  # 计算未来n日涨跌幅
    tezheng += [
        f'{n*10}日最高开盘价比值',
        f'{n*10}日最低开盘价比值',
        f'SMA{n*10}开盘比值',
        f'SMA{n*10}昨日成交额比值',]
# 对于每个时间戳在60分钟内的数据，找到其中最高价和最低价以及它们出现的时间戳。
mubiao = ['未来60日最高开盘价', '未来60日最低开盘价', '未来60日最高开盘价日期', '未来60日最低开盘价日期']
# 进行预测
x_pred = df[tezheng]
y_pred = model.predict(x_pred)
print(type(y_pred))
# 提取预测结果
predictions = pd.DataFrame({
    'timestamp': df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(int(x), tz=pytz.timezone('UTC')).astimezone(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')),
    '未来60日最高开盘价': y_pred[:, 0],
    '未来60日最低开盘价': y_pred[:, 1],
    '未来60日最高开盘价日期': y_pred[:, 2],
    '未来60日最低开盘价日期': y_pred[:, 3],
})
db[f'{name}预测'].drop()  # 清空集合中的所有文档
db[f'{name}预测'].insert_many(predictions.to_dict('records'))
# 发布到钉钉机器人
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'

for i in range(len(predictions)):
    message = f"产品名称：{name}\n预测日期(上海时间):{predictions['timestamp'][i]}\n'60步频最高开盘价':{predictions['未来60日最高开盘价'][i]}\n'60步频最低开盘价':{predictions['未来60日最低开盘价'][i]}\n'最高开盘价距离':{predictions['未来60日最高开盘价日期'][i]}\n'最低开盘价距离':{predictions['未来60日最低开盘价日期'][i]}"
    print(message)
    requests.post(webhook, json={
        'msgtype': 'text',
        'text': {
            'content': message
        }})
