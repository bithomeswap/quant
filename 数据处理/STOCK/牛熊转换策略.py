import pandas as pd

# name = 'STOCK_20140101_20170101'
# name = 'COIN'
name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]

n_stock = 100
df = df.groupby('日期').apply(lambda x: x.nlargest(
    n_stock, '开盘开盘幅')).reset_index(drop=True)
n_stock = 5
df = df.groupby('日期').apply(lambda x: x.nsmallest(
    n_stock, '开盘')).reset_index(drop=True)
if 'stock' in name.lower():
    df = df[
        (df['开盘收盘幅'] <= 8)
        &
        (df['开盘收盘幅'] >= 0)
    ]
    print('测试标的为股票类型，默认高开百分之八无法买入')

# 将交易标的细节输出到一个csv文件
trading_detail_filename = f'{name}交易标的细节.csv'
df.to_csv(trading_detail_filename, index=False)

# 假设开始时有10000元资金,实操时每个月还得归集一下资金，以免收益不平均
cash_balance = 10000
# 用于记录每日的资金余额
daily_cash_balance = {}
n = 1
# 设置持仓周期
m = 0
# 设置手续费

df_strategy = pd.DataFrame(columns=['日期', '执行策略'])
df_daily_return = pd.DataFrame(columns=['日期', '收益率'])

cash_balance_list = []

for date in sorted(df['日期'].unique()):
    group = df[df['日期'] == date]  # 当天的股票
    ema121_ratio_product = group['EMA121开盘比值'].prod()  # 当天所有股票EMA121开盘比值的乘积
    if ema121_ratio_product > 1.1:  # 牛市策略

    elif ema121_ratio_product < 0.9:  # 熊市策略

    else:  # 震荡策略
