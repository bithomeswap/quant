import asyncio
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
        # 计算昨日资金贡献
        df['昨日资金成本'] = df['昨日涨跌'] * df['昨日成交额']
        for n in range(1, 20):
            df[f'{n}日后总涨跌幅（未来函数）'] = (df['收盘'].copy().shift(-n)/df['收盘']) - 1
            df[f'{n}日后当日涨跌（未来函数）'] = df['涨跌幅'].copy().shift(-n)+1
    except Exception as e:
        print(f"发生bug: {e}")
    return df


def rank(df):  # 计算每个标的的各个指标在当日的排名，并将排名映射到 [0, 1] 的区间中
    # 计算每个指标的排名
    for column in df.columns:
        if '未来函数' not in str(column):
            df = pd.concat([df, (df[str(column)].rank(
                method='max', ascending=False) / len(df)).rename(f'{str(column)}_rank')], axis=1)
    return df


def tradelist(name):
    # 连接MongoDB数据库
    client = MongoClient(
        'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
    db = client['wth000']
    collection = db[f'{name}']
    # 获取数据并转换为DataFrame格式
    data = pd.DataFrame(list(collection.find()))
    print(f'{name}数据读取成功')
    # 按照“代码”列进行分组并计算技术指标
    df = data.groupby('代码', group_keys=False).apply(technology)
    # 分组并计算指标排名
    df = df.groupby(['日期'], group_keys=False).apply(rank)
    try:
        # 今日筛选股票推送(多头)
        df = df.sort_values(by='日期')
        # 获取最后一天的数据
        last_day = df.iloc[-1]['日期']
        # 计算总共统计的股票数量
        df = df[df[f'日期'] == last_day].copy()
        m = 0.001  # 设置手续费
        n = 6  # 设置持仓周期
        if ('etf' in name.lower()):  # 绝大部分基金靠收管理费赚钱，并不靠净值分红赚钱，分红赚的不如接盘私募多，所以这种衍生品比较弱势
            if ('分钟' not in name.lower()):
                df = df[df[f'真实价格'] >= 0.5].copy()  # 真实价格过滤劣质股票
                # 开盘收盘幅过滤涨停无法买入股票
                df = df[(df['开盘收盘幅'] <= 0.08)].copy()
                df = df[(df['昨日涨跌_rank'] >= 0.5)].copy()
                df = df[(df[f'过去{40}日总涨跌_rank'] >= 0.9)].copy()
                for n in range(2, 4):
                    df = df[(df[f'过去{n}日总涨跌'] <= 1)].copy()
                m = 0.005  # 设置手续费
                n = 4  # 设置持仓周期
            print(len(df), name)
        if ('coin' in name.lower()):
            if ('分钟' not in name.lower()):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
                df = df[(df['开盘_rank'] >= 0.5)].copy()  # 真实价格过滤劣质股票
                df = df[(df['昨日资金波动_rank'] <= 0.15)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 0.15)].copy()
                # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
                # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
                m = 0.003  # 设置手续费
                n = 6  # 设置持仓周期
            if ('分钟' in name.lower()):
                df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 6  # 设置持仓周期
            print(len(df), name)
        if ('证' in name.lower()) or ('test' in name.lower()):
            if ('分钟' not in name.lower()):
                df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
                # 开盘收盘幅过滤涨停无法买入股票
                df = df[(df['开盘收盘幅'] <= 0.01)].copy()
                df = df[(df['真实价格_rank'] <= 0.8) & (
                    df['真实价格_rank'] >= 0.2)].copy()  # 真实价格过滤劣质股票
                df = df[(df['昨日资金波动_rank'] <= 0.05)].copy()
                df = df[(df['昨日资金贡献_rank'] <= 0.05)].copy()
                # df = df[(df['昨日振幅_rank'] >= 0.1) & (df[f'昨日振幅_rank'] <= 0.9)].copy()
                # df = df[(df['昨日涨跌_rank'] >= 0.1) & (df[f'昨日涨跌_rank'] <= 0.9)].copy()
                m = 0.005  # 设置手续费
                n = 15  # 设置持仓周期
            if ('分钟' in name.lower()):
                df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
                for n in (2, 9):
                    df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
                    df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
                m = 0.0000  # 设置手续费
                n = 15  # 设置持仓周期
            print(len(df), name)
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
    df.to_csv(file_path, index=False)
    print(f'{name}准备插入数据')
    # 将数据分批插入
    batch_size = 20000  # 批量插入的大小
    num_batches = len(df) // batch_size + 1
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        data_slice = df[start_idx:end_idx]
        new_collection.insert_many(data_slice.to_dict('records'))
    print(f'{name}数据插入结束')


names = ['000', '001', '002', '600', '601', '603', '605', '指数', 'ETF', 'COIN']
for name in names:
    tradelist(name)
