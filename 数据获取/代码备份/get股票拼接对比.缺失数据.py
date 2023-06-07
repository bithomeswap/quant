import math
import choose
import pandas as pd
import os
import datetime
from pymongo import MongoClient
client = MongoClient(
    "mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000")
db = client["wth000"]
moneyused = 0.9  # 设置资金利用率
# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
path1 = os.path.join(
    dir_path, f"股票已拼接('000', '001', '002', '600', '601', '603', '605')指标.csv")
path2 = os.path.join(
    dir_path, f"股票('000', '001', '002', '600', '601', '603', '605')指标周期30交易细节.csv")
df1 = pd.read_csv(path1)
df2 = pd.read_csv(path2)
list1 = df1["日期"].copy().drop_duplicates().tolist()  # 获取所有不重复日期
list2 = df2["日期"].copy().drop_duplicates().tolist()  # 获取所有不重复日期
set1 = set(list1)
set2 = set(list2)
set_diff = set1.symmetric_difference(set2)
diff_list = list(set_diff)
print(diff_list)
diff_list=pd.DataFrame(diff_list)
diff_list.to_csv("差异数据")