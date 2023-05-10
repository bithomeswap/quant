from typing import List, Dict, Callable

import pandas as pd
import numpy as np
import requests
import json
import datetime
import bisect
import math
import copy

# 设定最大显示行数、列数为200
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)


def loop(dates: List[str], spaces: List[float], profit_days: List[int] = None):
    """
    收益计算demo
    :param dates: 日期列表 ['000001', '000002']
    :param spaces: 区间列表 [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5]
    :param profit_days: 持仓天数 [1, 3, 5]
    :return:
    """
    # 初始化变量
    if profit_days is None:
        profit_days = [1, 3, 5]  # 持仓天数
    daily_profit: Dict[str, Dict] = dict()  # 每日收益
    order_dict: Dict[str, Dict] = dict()  # 历史委托记录 {'2022-01-01':{'000001':{'space':2.0,'open':12.26,'day':0}}}
    space_profit: Dict[float, Dict] = dict()  # 不同区间统计 {1.5:{"profit": 0.0, "loss": 0, 'rate': 0.0, 'win': 0}, 2.0..}
    init_fund = 5211.29

    for space in spaces:
        temp = dict()
        for day in profit_days:
            temp[str(day)] = {"profit": 0.0, "loss": 0, 'rate': 0.0, 'win': 0}
        space_profit[space] = temp

    for today in dates:
        # 打印时间 日期
        print(datetime.datetime.now(), today)
        order_dict[today] = dict()

        # 获取当日股票列表和kline数据
        stock_list: List[str] = get_stock(today)
        data_day: Dict[str, pd.DataFrame] = get_kline(stock_list, today, 41)
        # buy_func(stock_list, data_day, order_dict)

        # 买入逻辑
        for stock in stock_list:
            df_data = data_day.get(stock)
            if df_data is None:
                continue
            df_turnover = pd.to_numeric(df_data['turnoverRate'])
            df_close = pd.to_numeric(df_data['close'])
            if len(df_data) == 41:
                avg = df_turnover[0:40].mean()
                today_turnover = df_turnover[40]
                rate = round(today_turnover / avg, 2)
                # 确定区间 二分查找
                index = bisect.bisect(spaces, rate)
                if index > 0:
                    order_dict[today][stock] = {'space': spaces[index - 1], 'open': buy_price(df_close[40]), 'day': 0}

        # 计算每只买入资金
        today_buy = pd.DataFrame(data=order_dict[today])
        if len(today_buy) > 0:
            counts = today_buy.T['space'].value_counts()
            counts_dict = counts.to_dict()
            for sotck, item in order_dict[today].items():
                item['funds'] = init_fund / counts_dict[item['space']]
        # 统计数据
        for date in order_dict.keys():
            tmp = order_dict[date]
            temp_keys = list(tmp.keys())
            for stock in temp_keys:
                # {'space': rate_vol, 'open': df_close, 'day': 0, 'funds':0}
                position = tmp[stock]
                df_data = data_day.get(stock)
                if df_data is None:
                    continue
                # 取当日收盘价计算卖出
                today_price = sell_price(pd.to_numeric(df_data['close'])[40])
                open_price = position['open']
                profit_data = space_profit[position['space']]
                for day in profit_days:
                    if position['day'] == day:
                        if today_price > open_price:
                            profit_data[str(day)]['win'] += 1
                        else:
                            profit_data[str(day)]['loss'] += 1
                        # fee = sell_fee((position['funds'] / open_price) * today_price)
                        profit_data[str(day)]['profit'] += ((today_price - open_price) / open_price) * position['funds']

                        profit_data[str(day)]['rate'] = profit_data[str(day)]['profit'] / init_fund
                        # profit_data[str(day)]['rate'] += (today_price - open_price) / open_price

                # 增加天数
                position['day'] += 1
                # 删除 day > 5
                if position['day'] > 5:
                    tmp.pop(stock)
        # 保存收益
        daily_profit[today] = copy.deepcopy(space_profit)

    # 保存到文件
    with open("daily.profit.json", "w") as f:
        json.dump(daily_profit, f)


def buy_price(price: float, slippage: float = 0.001) -> float:
    """
    买入价格计算
    :param price: 当前价格
    :param slippage: 滑点
    :return:
    """
    return math.ceil(price * (1 + slippage) * 100) / 100
    # return round(price * (1 + slippage), 2)


def sell_price(price: float, slippage: float = 0.001, commission: float = 0.0013) -> float:
    """
    卖出价格计算 去除滑点和手续费
    :param price: 当前价格
    :param slippage: 滑点
    :param commission: 手续费
    :return:
    """
    return math.floor(price * (1 - slippage) * (1 - commission) * 100) / 100


# def sell_fee(amount: float, commission: float = 0.0013) -> float:
#     """
#     手续费
#     :param amount: 成交金额
#     :param commission: 买卖费率
#     :return:
#     """
#     fee = round(amount * commission, 2)
#     if fee < 5:
#         fee = 5
#     return fee


def creat_excel(sheet_data: Dict[str, Dict], sheet_name: str, space_list: List[float],
                col_name: List[str] = None, profit_days: List[int] = None) -> pd.DataFrame:
    """
    生成excel
    :param sheet_data:
    :param sheet_name:
    :param space_list:
    :param col_name:
    :param profit_days:
    :return:
    """
    # 自定义字段名称
    if col_name is None:
        col_name = ['1天', '3天', '5天']
    # 持仓天数
    if profit_days is None:
        profit_days = [1, 3, 5]
    dates = sheet_data.keys()
    dict1 = dict()

    for key, item in sheet_data.items():
        for space in space_list:
            for name in col_name:
                sheet_col1 = f"{space}-{name}"
                day = profit_days[col_name.index(name)]
                if not dict1.keys().__contains__(sheet_col1):
                    dict1[sheet_col1] = list()
                dict1[sheet_col1].append(round(item[str(space)][str(day)]['rate'] * 100, 2))
    df = pd.DataFrame.from_dict(dict1)
    df.index = pd.to_datetime(list(dates)).strftime('%Y%m%d')
    # df.index = dates
    df.index.name = 'time'
    col_num = 0
    for name in col_name:
        for space in space_list:
            tmp = df.pop(f"{space}-{name}")
            df.insert(col_num, f"{space}-{name}", tmp)
            col_num += 1
    df.to_excel(f"{sheet_name}.xlsx")
    return df


def get_stock(date: str) -> List[str]:
    return list()


def get_kline(codes, date, limit) -> Dict[str, pd.DataFrame]:
    url = "http://localhost:8080/kline/list"
    body = {
        "codes": codes,
        "endDate": date,
        "limit": limit,
    }
    # cookie = "token=code_space;"
    header = {
        # "cookie": cookie,
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
    }

    r = requests.post(url, json=body, headers=header)
    data_json = r.json()
    # print(data_json)
    if not data_json["data"]:
        return dict()

    kline_data = data_json["data"]
    ret = dict()
    for k in kline_data.keys():
        ret[k] = pd.DataFrame(kline_data[k])

    return ret


if __name__ == '__main__':
    pass
    # <editor-fold dec='收益数据'>
    with open("json_text/daily.profit.json") as f:
        profit_data = json.load(f)
    # </editor-fold>
    space_list = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5]
    col_name = ['1天', '3天', '5天']
    profit_days = [1, 3, 5]
    start = datetime.datetime.now()
    df_temp = creat_excel(profit_data, sheet_name='2023-03-01至2023-04-03突破前高', space_list=space_list,
                          col_name=col_name, profit_days=profit_days)
    print(df_temp)
    end = datetime.datetime.now()
    print((end - start))

    print(buy_price(4.32))  # 4.33
    print(buy_price(9.79))  # 9.8
    print(buy_price(396.88))  # 397.28
    print(buy_price(549.9))  # 550.45
    print(buy_price(1705))  # 1706.71

    print(sell_price(4.32))  # 4.31
    print(sell_price(9.79))  # 9.76
    print(sell_price(396.88))  # 395.96
    print(sell_price(549.9))  # 548.63
    print(sell_price(1705))  # 1701.08

    # print(sell_fee(3000))  # 5
    # print(sell_fee(5000))  # 6.5
