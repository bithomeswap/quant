import os
import pandas as pd
# names = ["可转债","COIN", "股票", "指数", "行业", "ETF",]
names = ["股票", ]
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
files = os.listdir(dir_path)
for file in files:
    for filename in names:
        if (filename in file):
            try:
                # 获取文件名和扩展名
                name, extension = os.path.splitext(file)
                path = os.path.join(dir_path, f"{name}.csv")
                print(name)
                df = pd.read_csv(path)
                df = df[df['代码'] == '000993.XSHE']
                df.to_csv('test.csv')
            except Exception as e:
                print(f"发生bug: {e}")
