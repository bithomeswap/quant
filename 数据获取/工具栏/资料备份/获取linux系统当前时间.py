import ntplib
from datetime import datetime, timedelta

# 连接 NTP 服务器
ntp_client = ntplib.NTPClient()
response = ntp_client.request('pool.ntp.org')

# 解析时间戳和时间对象
timestamp = response.tx_time
dt = datetime.fromtimestamp(timestamp)

# 定义时间格式化字符串
fmt = '%Y-%m-%d %H:%M:%S'

# 输出当前时间字符串和时间戳
now_str = dt.strftime(fmt)
now_ts = int(timestamp)*1000
print(f'当前时间：{now_str}，时间戳：{now_ts}')

# 输出30天前的时间字符串和时间戳
before_30_days = dt - timedelta(days=30)
before_30_days_str = before_30_days.strftime(fmt)
before_30_days_ts = int(before_30_days.timestamp())*1000
print(f'30天前时间：{before_30_days_str}，时间戳：{before_30_days_ts}')
