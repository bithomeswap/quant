import os
import pandas as pd
import numpy as np
from choose import choose

# 设定参数起始值
w = 1.0
v = 1.0

# 设置基本路径
file_path = os.path.abspath(__file__)
dir_path = os.path.dirname(file_path)
dir_path = os.path.dirname(os.path.dirname(dir_path))

# 获取数据文件名列表
files = os.listdir(dir_path)
data_files = [filename for filename in files if '指标' in filename and '排名' not in filename and '细节' not in filename and '分钟' not in filename]

# 定义一个函数用于计算每天的收益率并返回最终收益率
def calculate_daily_ret(df, dates, w, v):
    result = []
    cash_balance = 1
    daily_cash_balance = {}
    
    for date in dates:
        day = dates.index(date)
        days = dates[day-n:day]
        daydf = df.loc[df['日期'].isin(days)].copy()
        daydf = daydf.fillna(1)
        daydf = daydf.sort_values(by='日期')
        daydates = daydf['日期'].copy().drop_duplicates().tolist()
        daily_ret = 0
        m = m/n
        
        # 计算每天的收益率
        if daydates:
            for i in range(0, len(daydates)-1):
                ret = daydf[daydf['日期'] == daydates[i]][f'{i+1}日后当日涨跌（未来函数）'].mean()*(1-m)-1
                daily_ret += ret/n
                
        # 计算每天的现金余额
        cash_balance *= (1 + daily_ret)
        daily_cash_balance[date] = cash_balance
        result.append({'日期': date, f'盘中波动': cash_balance})
        
    result_df = pd.DataFrame(result)
    result_df.to_csv(f'{name}_res_{w}_{v}.csv', index=False)
    return result_df.iloc[-1][f'盘中波动'] - 1

# 遍历所有数据文件，计算最优参数并输出结果
for data_file in data_files:
    name, extension = os.path.splitext(data_file)
    path = os.path.join(dir_path, f'{name}.csv')
    print(name)
    
    # 读取数据
    df = pd.read_csv(path)
    df = df.sort_values(by='日期')
    dates = df['日期'].copy().drop_duplicates().tolist()
    df = df.groupby(['代码'], group_keys=False).apply(choose.technology)
    for n in range(1, 9):
        df = df[df[f'{n}日后总涨跌幅（未来函数）'] <= 3*(1+n*0.2)]
    m = 0.001
    n = 6
    df, m, n = choose('交易', name, df)
    df.to_csv(f'{name}_交易细节.csv', index=False)
    
    # 初始化参数和结果
    result_df = pd.DataFrame({})
    max_ret = calculate_daily_ret(df, dates, w, v)
    best_w = w
    best_v = v
    
    # 寻找最优参数
    while (w > 0) and (v > 0):
        df_copy = df[(df['昨日资金波动_rank'] <= w*value/rank)].copy()
        df_copy = df_copy[(df_copy['昨日资金贡献_rank'] <= v*value/rank)].copy()
        ret = calculate_daily_ret(df_copy, dates, w, v)
        
        # 找到更好的参数则更新
        if ret > max_ret:
            max_ret = ret
            best_w = w
            best_v = v
        
        # 调整参数
        if w > v:
            w -= 0.05
        else:
            v -= 0.05
            
    # 保存最优结果
    df_best = df[(df['昨日资金波动_rank'] <= best_w*value/rank)].copy()
    df_best = df_best[(df_best['昨日资金贡献_rank'] <= best_v*value/rank)].copy()
    df_best.to_csv(f'{name}_best.csv', index=False)
    print(f'Best Params: W={best_w}, V={best_v}')
    print(f'Max Return: {max_ret*100}%')