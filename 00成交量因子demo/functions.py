'''
邢不行 | 量化小讲堂系列
《如何把交易经验转化为量化策略？以成交量为例【量化交易邢不行啊】》
https://www.bilibili.com/video/BV1Sy4y1s7dF
获取更多量化文章，请联系邢不行个人微信：xbx1717
'''
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 评价策略
def evaluate_investment(source_data, title, time_label='candle_end_time'):
    temp = source_data.copy()
    # ===新建一个dataframe保存回测指标
    results = pd.DataFrame()

    # ===计算累积净值
    results.loc[0, '累积净值'] = round(temp[title].iloc[-1], 2)

    # ===计算年化收益
    annual_return = (temp[title].iloc[-1]) ** (
            '1 days 00:00:00' / (temp[time_label].iloc[-1] - temp[time_label].iloc[0]) * 365) - 1
    results.loc[0, '年化收益'] = str(round(annual_return * 100, 2)) + '%'

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    temp['max2here'] = temp[title].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    temp['dd2here'] = temp[title] / temp['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(temp.sort_values(by=['dd2here']).iloc[0][[time_label, 'dd2here']])
    # 计算最大回撤开始时间
    start_date = temp[temp[time_label] <= end_date].sort_values(by=title, ascending=False).iloc[0][
        time_label]
    # 将无关的变量删除
    temp.drop(['max2here', 'dd2here'], axis=1, inplace=True)
    results.loc[0, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[0, '最大回撤开始时间'] = str(start_date)
    results.loc[0, '最大回撤结束时间'] = str(end_date)

    # ===年化收益/回撤比：我个人比较关注的一个指标
    results.loc[0, '年化收益/回撤比'] = round(annual_return / abs(max_draw_down), 2)

    return results.T

def draw_equity_curve(df, time, data_dict, pic_size=[18, 9], dpi=72, font_size=25):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.style.use('dark_background')
    plt.figure(figsize=(pic_size[0], pic_size[1]), dpi=dpi)
    plt.xticks(fontsize=font_size)
    plt.yticks(fontsize=font_size)
    for key in data_dict:
        plt.plot(df[time], df[data_dict[key]], label=key)
    plt.legend(fontsize=font_size)
    plt.show()
