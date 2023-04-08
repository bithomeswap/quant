from pymongo import MongoClient
import pandas as pd

# MongoDB数据库配置
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']
name = 'COIN指标'
collection = db[f"{name}"]

# 读取MongoDB中的数据
data = list(collection.find())
df = pd.DataFrame(data)
print('数据读取成功')
# 将结果保存为本地CSV文件
df.to_csv(f"{name}.csv", index=False)
