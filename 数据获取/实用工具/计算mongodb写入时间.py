from datetime import datetime
from bson import ObjectId

# 把 "641c00c06ff2a3207e45b117" 替换成你想查询的 ObjectId 字符串
oid_str = "6448fa0f4cd8271724205ed4"
oid = ObjectId(oid_str)
timestamp = int(str(oid)[:8], 16)  # 获取时间戳，转换为整型
write_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
print(write_time)
