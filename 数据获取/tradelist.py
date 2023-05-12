# import pytz
# import datetime
# import math
# import requests
# import pandas as pd
# import talib
# import os
# from pymongo import MongoClient


# def tradelist(df, name):
#     # 今日筛选股票推送(多头)
#     df = df.sort_values(by='日期')
#     # 获取最后一天的数据
#     last_day = df.iloc[-1]['日期']
#     # 计算总共统计的股票数量
#     df = df[df[f'日期'] == last_day].copy()
#     if ('coin' in name.lower()):
#         if ('分钟' not in name.lower()):
#             df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
#             df = df[df[f'昨日成交额'] >= 1000000].copy()  # 昨日成交额过滤劣质股票
#             df = df[(df['开盘_rank'] >= 0.5)].copy()  # 真实价格过滤劣质股票

#             df = df[(df['昨日振幅_rank'] >= 0.1) & (
#                 df[f'昨日振幅_rank'] <= 0.9)].copy()
#             df = df[(df['昨日涨跌_rank'] >= 0.1) & (
#                 df[f'昨日涨跌_rank'] <= 0.9)].copy()
#             df = df[(df['昨日资金波动_rank'] <= 0.5)].copy()  
#             df = df[(df['昨日资金贡献_rank'] <= 0.5)].copy()  
#             df = df[(df['昨日资金成本_rank'] >= 0.5)].copy()  
#             df = df[(df['昨日成交额_rank'] >= 0.5)].copy()  
#             for n in (2, 9):
#                 df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.1) &
#                         (df[f'过去{n}日总涨跌_rank'] <= 0.9)].copy()
#                 df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.1) &
#                         (df[f'过去{n}日总涨跌_rank'] <= 0.9)].copy()
#                 df = df[(df[f'过去{n}日累计昨日资金贡献_rank'] >= 0.5)].copy()
#                 df = df[(df[f'过去{n}日累计昨日成交额_rank'] <= 0.5)].copy()
#                 df = df[(df[f'过去{n}日累计昨日资金波动_rank'] >= 0.5)].copy()
#                 df = df[(df[f'过去{n}日累计昨日资金成本_rank'] <= 0.5)].copy()

#             m = 0.003  # 设置手续费
#             n = 6  # 设置持仓周期
#         if ('分钟' in name.lower()):
#             df = df[df[f'开盘'] >= 0.00001000].copy()  # 开盘价过滤高滑点股票
#             for n in (2, 9):
#                 df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
#                 df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
#             m = 0.0000  # 设置手续费
#             n = 6  # 设置持仓周期
#         print(len(df), name)
#     if ('证' in name.lower()):
#             if ('分钟' not in name.lower()):
#                 df = df[(df['真实价格'] >= 4)].copy()  # 真实价格过滤劣质股票
#                 df = df[(df['开盘收盘幅'] <= 0.01)].copy()  # 开盘收盘幅过滤涨停无法买入股票
#                 df = df[(df['真实价格_rank'] <= 0.8)].copy()  # 真实价格过滤劣质股票
#                 df = df[(df['真实价格_rank'] >= 0.2)].copy()  # 真实价格过滤劣质股票

#                 df = df[(df['昨日振幅_rank'] >= 0.1) & (
#                     df[f'昨日振幅_rank'] <= 0.9)].copy()
#                 df = df[(df['昨日涨跌_rank'] >= 0.1) & (
#                     df[f'昨日涨跌_rank'] <= 0.9)].copy()
#                 df = df[(df['昨日资金波动_rank'] <= 0.5)].copy()  
#                 df = df[(df['昨日资金贡献_rank'] <= 0.5)].copy()  
#                 df = df[(df['昨日资金成本_rank'] >= 0.5)].copy()  
#                 df = df[(df['昨日成交额_rank'] >= 0.5)].copy()  
#                 for n in (2, 9):
#                     df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.1) &
#                             (df[f'过去{n}日总涨跌_rank'] <= 0.9)].copy()
#                     df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.1) &
#                             (df[f'过去{n}日总涨跌_rank'] <= 0.9)].copy()
#                     df = df[(df[f'过去{n}日累计昨日资金贡献_rank'] >= 0.5)].copy()
#                     df = df[(df[f'过去{n}日累计昨日成交额_rank'] <= 0.5)].copy()
#                     df = df[(df[f'过去{n}日累计昨日资金波动_rank'] >= 0.5)].copy()
#                     df = df[(df[f'过去{n}日累计昨日资金成本_rank'] <= 0.5)].copy()
#                 m = 0.005  # 设置手续费
#                 n = 18  # 设置持仓周期
#             if ('分钟' in name.lower()):
#                 df = df[(df['开盘'] >= 4)].copy()  # 真实价格过滤劣质股票
#                 for n in (2, 9):
#                     df = df[(df[f'过去{n}日总涨跌_rank'] >= 0.5)].copy()
#                     df = df[(df[f'过去{n*5}日总涨跌_rank'] >= 0.5)].copy()
#                 m = 0.0000  # 设置手续费
#                 n = 18  # 设置持仓周期
#             print(len(df), name)

#     # 发布到钉钉机器人
#     df['市场'] = name
#     print(df)
#     message = df[['市场', '代码', '日期', '开盘']].to_markdown()
#     print(type(message))
#     webhook = 'https://oapi.dingtalk.com/robot/send?access_token=f5a623f7af0ae156047ef0be361a70de58aff83b7f6935f4a5671a626cf42165'
#     requests.post(webhook, json={'msgtype': 'markdown', 'markdown': {
#         'title': f'{name}', 'text': message}})
