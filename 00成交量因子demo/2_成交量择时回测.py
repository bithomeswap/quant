'''
邢不行 | 量化小讲堂系列
《如何把交易经验转化为量化策略？以成交量为例【量化交易邢不行啊】》
https://www.bilibili.com/video/BV1Sy4y1s7dF
获取更多量化文章，请联系邢不行个人微信：xbx1717
'''
from functions import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

index = 'sh000300'
time_label = 'candle_end_time'
threshold = 0.2
n_days = 40  # 回溯的样本天数
rate = 1.2 / 10000
# 读取文件
df = pd.read_csv('data/%s_bt.csv' % index, encoding='gbk', parse_dates=[time_label])
df['成交量择时策略净值'] = (df['最终涨跌幅'] + 1).cumprod()
res = evaluate_investment(df, title='成交量择时策略净值')
draw_equity_curve(df, data_dict={'成交量择时策略': '成交量择时策略净值', '沪深300': '资金曲线'}, time='candle_end_time')
print(res)
