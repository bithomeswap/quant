import pandas as pd
import decimal
# # 开方
# result = (9.95*497) ** (1/3)
# print(result)

# 科学计数法
x = 8.89877275025826E-10
print(float(x))
print(decimal.Decimal(x))
print('{:.15f}'.format(x))

# # 最小精度位加减
# from decimal import Decimal, getcontext
# num = 0.12219
# def add_one_to_float(num):
#     # 将浮点数转化为字符串
#     str_num = str(num)
#     # 找到最后一位非零数位的位置
#     index = len(str_num) - 1
#     while index >= 0 and (str_num[index] == '0' or str_num[index] == '.'):
#         index -= 1
#     # 将该位置上的字符转换为数字，并加一
#     new_num = float(str_num[:index] +
#                     str(int(str_num[index]) + 1) + str_num[index+1:])
#     # 返回加一后的浮点数
#     return new_num
# print(add_one_to_float(num))

# # 时间戳过滤
# df = pd.DataFrame({
#     "日期": ['2021-09-09'],
#     "timestamp": [1682726519.999],
#     "开盘收盘幅": [-0.00141304694037703],
#     "昨日振幅": [0.032712174840637],
#     "昨日成交额": [2606152384],
#     "delta开盘": [-29.2599999999998]
# }, index=[0])
# # 过滤条件：不在0、8、14整点时间之前20分钟的数据（时间戳对应的标准时间）
# df['资金结算'] = pd.to_datetime(df['timestamp'], unit='s')
# df = df[df['资金结算'].apply(lambda x: not ((x.hour in [7, 15, 23]) and (x.minute > 40)))]
# print(df)

# # 时间戳计算（上海时间与秒级时间戳）
# import datetime
# timestamp = 1682584200
# dt = datetime.datetime.fromtimestamp(
#     timestamp, tz=datetime.timezone(datetime.timedelta(hours=8)))
# print(dt)
