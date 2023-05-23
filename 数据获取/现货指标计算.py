import choose
import requests
import pandas as pd
import os
from pymongo import MongoClient


def technology(df):  # 定义计算技术指标的函数
    try:
        df = df.dropna()  # 删除缺失值，避免无效数据的干扰
        # 删除最高价和最低价为负值的数据
        df.drop(df[(df['最高'] < 0) | (df['最低'] < 0)].index, inplace=True)
        df.sort_values(by='日期')    # 以日期列为索引,避免计算错误
        # 定义开盘收盘幅
        df['开盘收盘幅'] = df['开盘']/df['收盘'].copy().shift(1) - 1
        # 计算涨跌幅
        df['涨跌幅'] = df['收盘']/df['收盘'].copy().shift(1) - 1
        # 计算昨日振幅
        df['昨日振幅'] = (df['最高'].copy().shift(
            1)-df['最低'].copy().shift(1))/df['开盘'].copy().shift(1)
        # 计算昨日成交额
        df['昨日成交额'] = df['成交额'].copy().shift(1)
        # 计算昨日涨跌
        df['昨日涨跌'] = df['涨跌幅'].copy().shift(1)+1
        # 计算昨日资金贡献
        df['昨日资金贡献'] = df['昨日涨跌'] / df['昨日成交额']
        # 计算昨日资金波动
        df['昨日资金波动'] = df['昨日振幅'] / df['昨日成交额']
        if ('分钟' in name) | ('指数' in name) | ('行业' in name):
            for n in range(1, 10):
                df[f'过去{n}日总涨跌'] = df['开盘']/(df['开盘'].copy().shift(n))
                df[f'过去{n*5}日总涨跌'] = df['开盘']/(df['开盘'].copy().shift(n*5))
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def rank(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if ('未来函数' not in str(column)):
            df = pd.concat([df, (df[str(column)].rank(
                method='max', ascending=False) / len(df)).rename(f'{str(column)}_rank')], axis=1)
    return df


def tradelist(name):
    collection = db[f'{name}']
    # 获取数据并转换为DataFrame格式
    data = pd.DataFrame(list(collection.find()))
    print(f'{name}数据读取成功')
    # 按照“代码”列进行分组并计算技术指标
    data = data.groupby(['代码'], group_keys=False).apply(technology)
    # 分组并计算指标排名
    data = data.groupby(['日期'], group_keys=False).apply(rank)
    try:
        df = data.sort_values(by='日期')
        # 获取最后一天的数据
        last_day = df.iloc[-1]['日期']
        # 计算总共统计的股票数量
        df = df[df[f'日期'] == last_day].copy()
        df, m, n = choose.choose('交易', name, df)
        if len(df) < 200:
            # 发布到钉钉机器人
            df['市场'] = name
            print(df)
            message = df[['市场', '代码', '日期', '开盘']].to_markdown()
            print(type(message))
            webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
            requests.post(webhook, json={'msgtype': 'markdown', 'markdown': {
                'title': f'{name}', 'text': message}})
    except Exception as e:
        print(f"发生bug: {e}")
    # 连接MongoDB数据库并创建新集合
    new_collection = db[f'{name}指标']
    new_collection.drop()  # 清空集合中的所有文档
    # 获取当前.py文件的绝对路径
    file_path = os.path.abspath(__file__)
    # 获取当前.py文件所在目录的路径
    dir_path = os.path.dirname(file_path)
    # 获取当前.py文件所在目录的上两级目录的路径
    dir_path = os.path.dirname(os.path.dirname(dir_path))
    # 保存数据到指定目录
    file_path = os.path.join(dir_path, f'{name}指标.csv')
    data.to_csv(file_path, index=False)
    print(f'{name}准备插入数据')
    # new_collection.insert_many(data.to_dict('records'))
    print(f'{name}数据插入结束')


# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 获取当前数据库中的所有集合名称
names = list(db.list_collection_names())
print(names)
for name in names:
    if ('指标' not in name) & ('order' not in name) & ('js' not in name):
        # if ('分钟' not in name):
        # if ('分钟' in name):
        # if ('行业' in name) | ('指数' in name):
        # if ('股票' in name):
        # if ('COIN' in name):
        # if ('历史' in name):
        print(f'当前计算{name}')
        try:
            tradelist(name)
        except Exception as e:
            print(f"发生bug: {e}")
