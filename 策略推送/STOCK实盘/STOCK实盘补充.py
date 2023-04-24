import akshare as ak
from datetime import datetime


# 获取A股主板股票的代码和名称
stock_info_df = ak.stock_zh_a_spot_em()
# 过滤掉ST股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('ST')]
# 过滤掉退市股票
stock_info_df = stock_info_df[~stock_info_df['名称'].str.contains('退')]
stock_info_df = stock_info_df[stock_info_df['代码'].str.startswith(
    ('60', '000', '001'))]
stock_info_df['代码'] = stock_info_df["代码"].apply(lambda x: float(x))
stock_info_df["成交量"] = stock_info_df["成交量"].apply(lambda x: float(x))
stock_info_df['timestamp'] = stock_info_df['日期'].apply(lambda x: float(
    stock_info_df.datetime.strptime(x, '%Y-%m-%d').timestamp()))
# # 保留代码以及开盘价、最高价、最低价、收盘价字段
# today_data = today_data[['ts_code', 'open', 'high', 'low', 'close']]
# # 将ts_code重命名为代码
# today_data = today_data.rename(columns={'ts_code': '代码'})
# # 将数据类型转换为float
# today_data[['open', 'high', 'low', 'close']] = today_data[['open', 'high', 'low', 'close']].astype(float)
# 输出结果
print(stock_info_df)
