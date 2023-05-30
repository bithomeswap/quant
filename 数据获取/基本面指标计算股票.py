import choose
import requests
import pandas as pd
import os
from pymongo import MongoClient


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
    # 分组并计算指标排名
    data = data.groupby(['日期'], group_keys=False).apply(rank)
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
    new_collection.insert_many(data.to_dict('records'))
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
        if ('分钟' not in name):
            # if ('分钟' in name):
            # if ('行业' in name) | ('指数' in name):
            if ('股票基本面' in name):
                # if ('COIN' in name):
                # if ('历史' in name):
                print(f'当前计算{name}')
                try:
                    tradelist(name)
                except Exception as e:
                    print(f"发生bug: {e}")
