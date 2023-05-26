from pymongo import MongoClient
import pandas as pd
import os
# MongoDB数据库配置
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
# 设置参数
name = "股票基本面('000', '001', '002', '600', '601', '603', '605')"

collection = db[f"{name}"]
# 读取MongoDB中的数据
data = list(collection.find())
df = pd.DataFrame(data)
print('数据读取成功')

# 获取当前.py文件的绝对路径
file_path = os.path.abspath(__file__)
# 获取当前.py文件所在目录的路径
dir_path = os.path.dirname(file_path)
# 获取当前.py文件所在目录的上两级目录的路径
dir_path = os.path.dirname(os.path.dirname(dir_path))
file_path = os.path.join(dir_path, f'{name}.csv')
df.to_csv(file_path, index=False)
