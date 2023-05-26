import choose
import pandas as pd
import os
import datetime
names = ['股票']
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
firstpath = os.path.join(
    dir_path, f"股票基本面('000', '001', '002', '600', '601', '603', '605')指标.csv")
firstdf = pd.read_csv(firstpath)

for file in files:
    for filename in names:
        if (filename in file) & ('指标' in file) & ('基本面' not in file) & ('排名' not in file) & ('细节' not in file) & ('分钟' not in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f'{name}.csv')
                df = pd.read_csv(path)
                df = pd.merge(df, firstdf, on=[
                              '日期', '代码'], how='inner', suffixes=('_df1', '_df2'))
                print(df)
                path = os.path.join(os.path.abspath('.'), '资产股票数据（含基本面）')
                if not os.path.exists(path):
                    os.makedirs(path)
                df.to_csv(f'{path}/{name}（含基本面）.csv', index=False)
            except Exception as e:
                print(f"发生bug: {e}")
