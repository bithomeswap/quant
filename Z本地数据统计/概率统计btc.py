import pandas as pd

name = 'BTC'

# 读取数据
df = pd.read_csv(f'{name}指标.csv')

# 选取EMA9收盘动能3大于1.03且EMA121收盘比值小于0.5的数据
df = df[(df['EMA9收盘动能3'] > 1.03) & (df['EMA121收盘比值'] < 0.5)]

# 按日期排序
df = df.sort_values(by='日期')

# 将交易标的细节输出到csv文件
trading_detail_filename = f'{name}交易细节.csv'
df.to_csv(trading_detail_filename, index=False)
# 计算六分钟之后的收益率
n=6
# 计算收益率并输出到csv文件
df['收益率'] = df[f'{n}日后总涨跌幅（未来函数）']
profit_filename = f'{name}收益率.csv'
df[['日期', '收益率']].to_csv(profit_filename, index=False)
