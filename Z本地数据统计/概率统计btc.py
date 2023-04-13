import pandas as pd
name='BTC'
# 读取数据
df = pd.read_csv(f'{name}指标.csv')

# 去除空值
df = df.dropna()

# 选取符合交易机会的时间
df = df[(df['EMA9收盘动能3'] > 1.03) & (df['EMA121收盘比值'] < 0.5)]

# 按日期排序
df = df.sort_values('日期')

# 转换日期格式
df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')

# 计算持仓天数
hold_days = 10

# 计算每份资金
cash_per_share = 10000 / hold_days

# 初始化资产总价值和持仓股票数量
total_value = 10000
share_count = 0

# 初始化交易细节列表和收益率列表
trading_details = []
returns = []

# 遍历每一天
for i in range(len(df)):
    # 获取当天的数据
    curr_data = df.iloc[i]

    # 如果当天没有符合交易机会的股票，则跳过
    if pd.isnull(curr_data['开盘']):
        continue

    # 如果是新的一天则更新持有股票的数量
    if i > 0 and curr_data['日期'] != df.iloc[i-1]['日期']:
        share_count = int(total_value / curr_data['开盘'])
    
    # 判断是否需要卖出持仓股票
    if share_count > 0 and i > hold_days:
        sell_data = df.iloc[i-hold_days-1]
        total_value = share_count * sell_data['收盘']
        share_count = 0
        trading_details.append({
            '日期': sell_data['日期'].strftime('%Y%m%d'),
            '操作': '卖出',
            '股票代码': sell_data['股票代码'],
            '股票名称': sell_data['股票名称'],
            '价格': sell_data['收盘'],
            '数量': hold_days,
            '金额': total_value,
        })
    
    # 判断是否需要买入新的股票
    if total_value >= cash_per_share and share_count == 0:
        buy_data = curr_data
        share_count = int(cash_per_share / buy_data['开盘'])
        total_value -= share_count * buy_data['开盘']
        trading_details.append({
            '日期': buy_data['日期'].strftime('%Y%m%d'),
            '操作': '买入',
            '股票代码': buy_data['股票代码'],
            '股票名称': buy_data['股票名称'],
            '价格': buy_data['开盘'],
            '数量': share_count,
            '金额': cash_per_share,
        })
    
    # 计算收益率
    if i > hold_days:
        returns.append((total_value + share_count * curr_data['收盘']) / 10000 - 1)
    
# 输出交易细节
pd.DataFrame(trading_details).to_csv(f'{name}交易细节.csv', index=False)

# 输出收益率
pd.DataFrame({'日期': df['日期'][hold_days:], '收益率': returns}).to_csv(f'{name}收益率.csv', index=False)
