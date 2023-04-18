import pandas as pd

# name = 'STOCK_20140101_20170101'
# name = 'COIN'
name = 'STOCK'
df = pd.read_csv(f'{name}指标.csv')

# 去掉n日后总涨跌幅大于百分之三百的噪音数据
for n in range(1, 9):
    df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 300*(1+n*0.2)]
    
# 多选择几次集合，最后合并到一起其实就能实现