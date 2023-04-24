# result = (1.9*1.12) ** (1/3)
# print(result)
# result = (1.02) ** (12)
# print(result)


from decimal import Decimal, getcontext

num = 0.12219


def add_one_to_float(num):
    # 将浮点数转化为字符串
    str_num = str(num)
    # 找到最后一位非零数位的位置
    index = len(str_num) - 1
    while index >= 0 and (str_num[index] == '0' or str_num[index] == '.'):
        index -= 1
    # 将该位置上的字符转换为数字，并加一
    new_num = float(str_num[:index] +
                    str(int(str_num[index]) + 1) + str_num[index+1:])
    # 返回加一后的浮点数
    return new_num


print(add_one_to_float(num))
