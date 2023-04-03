'''
邢不行 | 量化小讲堂系列
《如何把交易经验转化为量化策略？以成交量为例【量化交易邢不行啊】》
https://www.bilibili.com/video/BV1Sy4y1s7dF
获取更多量化文章，请联系邢不行个人微信：xbx1717
'''
import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# 参数设置
index = 'sh000300'
time_label = 'candle_end_time'
n_days = 40  # 回溯的样本天数
# 读取文件
df = pd.read_csv('data/%s.csv' % index, encoding='gbk', parse_dates=[time_label])
# 计算标准成交量
df['rank'] = df['amount'].rolling(n_days + 1).apply(lambda x: pd.Series(x).rank().iloc[-1])
df['amount_standard_rank'] = 2 * (df['rank'] - n_days - 1) / n_days + 1
# 计算未来5天收益
df['未来5天收益'] = df['close'].shift(-5) / df['close'] - 1
# 删除空值
df.dropna(subset=['amount_standard_rank'], inplace=True)
df.dropna(subset=['未来5天收益'], inplace=True)
# 分析标准成交量和未来收益的关系
temp = df.groupby('amount_standard_rank')['未来5天收益'].mean()
temp = pd.DataFrame(temp)
temp.to_csv('data/%s_ad.csv' % index, encoding='gbk')
print(temp)
print(df.head(10))
